package nsqd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bitly/nsq/util"
	"github.com/bitly/nsq/util/registrationdb"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/serf/serf"
)

type logWriter struct {
	logger
	prefix []byte
}

func (l logWriter) Write(p []byte) (int, error) {
	if l.logger == nil {
		return 0, nil
	}
	if bytes.Contains(p, []byte("DEBUG")) {
		return 0, nil
	}
	p = bytes.TrimSpace(p)
	idx := bytes.Index(p, l.prefix)
	l.logger.Output(2, string(p[idx:]))
	return len(p), nil
}

type gossipEvent struct {
	Name    string `json:"n"`
	Topic   string `json:"t"`
	Channel string `json:"c"`
	Rnd     int64  `json:"r"`
}

func memberToProducer(member serf.Member) *registrationdb.Producer {
	tcpPort, _ := strconv.Atoi(member.Tags["tp"])
	httpPort, _ := strconv.Atoi(member.Tags["hp"])
	return &registrationdb.Producer{
		PeerInfo: &registrationdb.PeerInfo{
			ID: member.Name,
			RemoteAddress: net.JoinHostPort(member.Addr.String(),
				strconv.Itoa(int(member.Port))),
			LastUpdate:       time.Now().UnixNano(),
			BroadcastAddress: member.Tags["ba"],
			Hostname:         member.Tags["h"],
			TCPPort:          tcpPort,
			HTTPPort:         httpPort,
			Version:          member.Tags["v"],
		},
	}
}

func initSerf(opts *nsqdOptions,
	serfEventChan chan serf.Event,
	tcpAddr *net.TCPAddr, httpAddr *net.TCPAddr, httpsAddr *net.TCPAddr, broadcastAddr net.IP) (*serf.Serf, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	gossipAddr, err := net.ResolveTCPAddr("tcp", opts.GossipAddress)
	if err != nil {
		return nil, err
	}

	serfConfig := serf.DefaultConfig()
	serfConfig.Init()
	serfConfig.Tags["role"] = "nsqd"
	serfConfig.Tags["tp"] = strconv.Itoa(tcpAddr.Port)
	serfConfig.Tags["hp"] = strconv.Itoa(httpAddr.Port)
	if httpsAddr != nil {
		serfConfig.Tags["hps"] = strconv.Itoa(httpsAddr.Port)
	}
	serfConfig.Tags["ba"] = opts.BroadcastAddress
	serfConfig.Tags["h"] = hostname
	serfConfig.Tags["v"] = util.BINARY_VERSION
	serfConfig.NodeName = net.JoinHostPort(opts.BroadcastAddress, strconv.Itoa(tcpAddr.Port))

	serfConfig.MemberlistConfig = memberlist.DefaultLocalConfig()
	serfConfig.MemberlistConfig.AdvertiseAddr = broadcastAddr.String()
	serfConfig.MemberlistConfig.BindAddr = gossipAddr.IP.String()
	serfConfig.MemberlistConfig.BindPort = gossipAddr.Port
	serfConfig.MemberlistConfig.GossipInterval = 100 * time.Millisecond
	serfConfig.MemberlistConfig.GossipNodes = 5
	serfConfig.MemberlistConfig.LogOutput = logWriter{opts.Logger, []byte("memberlist:")}
	serfConfig.EventCh = serfEventChan
	serfConfig.EventBuffer = 1024
	serfConfig.ReconnectTimeout = time.Hour
	serfConfig.LogOutput = logWriter{opts.Logger, []byte("serf:")}

	return serf.Create(serfConfig)
}

func (n *NSQD) serfMemberJoin(ev serf.Event) {
	memberEv := ev.(serf.MemberEvent)
	n.logf("MEMBER EVENT: %+v - members: %+v", memberEv, memberEv.Members)
	for _, member := range memberEv.Members {
		producer := memberToProducer(member)
		r := registrationdb.Registration{"client", "", ""}
		n.rdb.AddProducer(r, producer)
		n.logf("DB(%s): member(%s) REGISTER %s", n.tcpListener.Addr(), producer.ID, r)
	}
}

func (n *NSQD) serfMemberFailed(ev serf.Event) {
	memberEv := ev.(serf.MemberEvent)
	n.logf("MEMBER EVENT: %+v - members: %+v", memberEv, memberEv.Members)
	for _, member := range memberEv.Members {
		registrations := n.rdb.LookupRegistrations(member.Name)
		for _, r := range registrations {
			if removed, _ := n.rdb.RemoveProducer(r, member.Name); removed {
				n.logf("DB: member(%s) UNREGISTER %s", member.Name, r)
			}
		}
	}
}

func (n *NSQD) serfUserEvent(ev serf.Event) {
	var gev gossipEvent
	var member serf.Member

	userEv := ev.(serf.UserEvent)
	err := json.Unmarshal(userEv.Payload, &gev)
	if err != nil {
		n.logf("ERROR: failed to Unmarshal gossipEvent - %s", err)
		return
	}

	n.logf("gossipEvent: %+v", gev)

	found := false
	for _, m := range n.serf.Members() {
		if m.Name == gev.Name {
			member = m
			found = true
		}
	}

	if !found {
		n.logf("ERROR: received gossipEvent for unknown node - %s", userEv.Name)
		return
	}

	producer := memberToProducer(member)
	operation := userEv.Name[len(userEv.Name)-1]
	switch operation {
	case '+', '=':
		n.gossipHandleCreateEvent(operation, producer, gev)
	case '-':
		n.gossipHandleDeleteEvent(operation, producer, gev)
	}
}

func (n *NSQD) gossipHandleCreateEvent(operation byte,
	producer *registrationdb.Producer, gev gossipEvent) {
	var registrations []registrationdb.Registration

	if gev.Channel != "" {
		registrations = append(registrations, registrationdb.Registration{
			Category: "channel",
			Key:      gev.Topic,
			SubKey:   gev.Channel,
		})
	}

	registrations = append(registrations, registrationdb.Registration{
		Category: "topic",
		Key:      gev.Topic,
	})

	for _, r := range registrations {
		if n.rdb.AddProducer(r, producer) {
			n.logf("DB: member(%s) REGISTER %s", gev.Name, r)
		}
		if operation == '=' && n.rdb.TouchProducer(r, producer.ID) {
			n.logf("DB: member(%s) TOUCH %s", gev.Name, r)
		}
	}
}

func (n *NSQD) gossipHandleDeleteEvent(operation byte,
	producer *registrationdb.Producer, gev gossipEvent) {
	if gev.Channel != "" {
		r := registrationdb.Registration{
			Category: "channel",
			Key:      gev.Topic,
			SubKey:   gev.Channel,
		}

		removed, left := n.rdb.RemoveProducer(r, producer.ID)
		if removed {
			n.logf("DB: member(%s) UNREGISTER %s", gev.Name, r)
		}

		// for ephemeral channels, if it has no producers, remove the registration
		if left == 0 && strings.HasSuffix(gev.Channel, "#ephemeral") {
			n.rdb.RemoveRegistration(r)
		}

		return
	}

	// this is a topic unregistration (no channel was specified)

	registrations := n.rdb.FindRegistrations("channel", gev.Topic, "*")
	for _, r := range registrations {
		// remove all of the channel registrations...
		if removed, _ := n.rdb.RemoveProducer(r, producer.ID); removed {
			// normally this shouldn't happen which is why we print a warning message
			// if anything is actually removed
			n.logf("WARNING: client(%s) unexpected UNREGISTER %s", gev.Name, r)
		}
	}

	r := registrationdb.Registration{
		Category: "topic",
		Key:      gev.Topic,
		SubKey:   "",
	}
	if removed, _ := n.rdb.RemoveProducer(r, producer.ID); removed {
		n.logf("DB: client(%s) UNREGISTER %s", gev.Name, r)
	}
}

func (n *NSQD) serfEventLoop() {
	for {
		select {
		case ev := <-n.serfEventChan:
			switch ev.EventType() {
			case serf.EventMemberJoin:
				n.serfMemberJoin(ev)
			case serf.EventMemberLeave:
				// nothing (should never happen)
			case serf.EventMemberFailed:
				n.serfMemberFailed(ev)
			case serf.EventMemberReap:
				// nothing
			case serf.EventUser:
				n.serfUserEvent(ev)
			case serf.EventQuery:
				// nothing
			case serf.EventMemberUpdate:
				// nothing
			default:
				n.logf("WARNING: un-handled Serf event: %#v", ev)
			}
		case <-n.exitChan:
			goto exit
		}
	}

exit:
	n.logf("SERF: exiting")
}

func (n *NSQD) gossip(evName string, topicName string, channelName string) error {
	gev := gossipEvent{
		Name:    n.serf.LocalMember().Name,
		Topic:   topicName,
		Channel: channelName,
		Rnd:     time.Now().UnixNano(),
	}
	payload, err := json.Marshal(&gev)
	if err != nil {
		return err
	}
	return n.serf.UserEvent(evName, payload, false)
}

func (n *NSQD) gossipLoop() {
	var evName string
	var topicName string
	var channelName string

	regossipTicker := time.NewTicker(60 * time.Second)

	if len(n.opts.SeedNodeAddresses) > 0 {
		for {
			num, err := n.serf.Join(n.opts.SeedNodeAddresses, false)
			if err != nil {
				n.logf("ERROR: failed to join serf - %s", err)
				select {
				case <-time.After(15 * time.Second):
					// keep trying
				case <-n.exitChan:
					goto exit
				}
			}
			if num > 0 {
				n.logf("SERF: joined %d nodes", num)
				break
			}
		}
	}

	for {
		select {
		case <-regossipTicker.C:
			n.logf("SERF: re-gossiping")
			stats := n.GetStats()
			for _, topicStat := range stats {
				if len(topicStat.Channels) == 0 {
					// if there are no channels we just send a topic exists event
					err := n.gossip("topic=", topicStat.TopicName, "")
					if err != nil {
						n.logf("ERROR: failed to send Serf user event - %s", err)
					}
					continue
				}
				// otherwise only send a channel, implying the existence of the topic
				for _, channelStat := range topicStat.Channels {
					err := n.gossip("channel=", topicStat.TopicName, channelStat.ChannelName)
					if err != nil {
						n.logf("ERROR: failed to send Serf user event - %s", err)
					}
				}
			}
		case v := <-n.gossipChan:
			switch v.(type) {
			case *Channel:
				channel := v.(*Channel)
				topicName = channel.topicName
				channelName = channel.name
				if !channel.Exiting() {
					evName = "channel+"
				} else {
					evName = "channel-"
				}
			case *Topic:
				topic := v.(*Topic)
				topicName = topic.name
				channelName = ""
				if !topic.Exiting() {
					evName = "topic+"
				} else {
					evName = "topic-"
				}
			}
			err := n.gossip(evName, topicName, channelName)
			if err != nil {
				n.logf("ERROR: failed to send Serf user event - %s", err)
			}
		case <-n.exitChan:
			goto exit
		}
	}

exit:
	regossipTicker.Stop()
	n.logf("GOSSIP: exiting")
}
