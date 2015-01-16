package nsqd

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/bitly/go-nsq"
)

func TestResilience(t *testing.T) {
	var nsqds []*NSQD
	var seedNode *NSQD
	var tcpPorts []int

	num := 3
	for i := 0; i < num; i++ {
		// find an open port
		tmpl, err := net.Listen("tcp", "127.0.0.1:0")
		equal(t, err, nil)
		addr := tmpl.Addr().(*net.TCPAddr)
		tmpl.Close()

		opts := NewNSQDOptions()
		opts.ID = int64(i)
		opts.Logger = newTestLogger(t)
		opts.GossipAddress = addr.String()
		opts.BroadcastAddress = "127.0.0.1"
		opts.gossipDelegate = convergenceTester
		if seedNode != nil {
			opts.SeedNodeAddresses = []string{seedNode.opts.GossipAddress}
		}
		tcpAddr, _, nsqd := mustStartNSQD(opts)
		nsqd.messageDelegate = &delegate{
			n: nsqd,
			a: newAuditor(nsqd),
		}
		if i > 0 {
			defer nsqd.Exit() // we will close the seed node manually
		}

		nsqds = append(nsqds, nsqd)
		tcpPorts = append(tcpPorts, tcpAddr.Port)

		if seedNode == nil {
			seedNode = nsqd
		}
	}

	// wait for convergence
	converge(5*time.Second, nsqds, func() bool {
		for _, nsqd := range nsqds {
			if len(nsqd.rdb.FindProducers("client", "", "")) != num {
				return false
			}
		}
		return true
	})

	// create a topic/channel on the first node
	topicName := "topic1"
	topic := nsqds[0].GetTopic(topicName)
	topic.GetChannel("ch")

	// wait for convergence
	converge(10*time.Second, nsqds, func() bool {
		for _, nsqd := range nsqds {
			if len(nsqd.rdb.FindProducers("topic", topicName, "")) != 1 ||
				len(nsqd.rdb.FindProducers("channel", topicName, "ch")) != 1 {
				return false
			}
		}
		return true
	})

	messageCount := 0

	// set up a client on the first node
	cons, err := nsq.NewConsumer(topicName, "ch", nsq.NewConfig())
	equal(t, err, nil)
	cons.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		fmt.Println(message.ID)
		messageCount++
		return nil
	}))
	err = cons.ConnectToNSQDs([]string{
		nsqds[0].tcpListener.Addr().String(),
		nsqds[1].tcpListener.Addr().String(),
		nsqds[2].tcpListener.Addr().String(),
	})
	equal(t, err, nil)

	// break the client's ability to pull messages
	t.Log("disabling message pulling")
	cons.ChangeMaxInFlight(0)
	time.Sleep(50 * time.Millisecond)

	// push message on first node, waiting for resilient message propagation
	url := "http://" + nsqds[0].httpListener.Addr().String() + "/pub?topic=" + topicName
	resp, err := http.Post(url, "application/octet-stream", bytes.NewBufferString("test"))
	equal(t, err, nil)

	// receive OK for publish
	equal(t, resp.StatusCode, http.StatusOK)
	resp.Body.Close()

	// kill the first node
	nsqds[0].Exit()
	time.Sleep(200 * time.Millisecond)

	// restore the client's ability to pull messages
	t.Log("restoring message pulling")
	cons.ChangeMaxInFlight(10)

	// check that client gets the message
	time.Sleep(5 * time.Second)
	equal(t, messageCount, 1)
}
