package nsqd

import (
	"net"
	"testing"
	"time"
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
		opts.gossipDelegate = tester
		if seedNode != nil {
			opts.SeedNodeAddresses = []string{seedNode.opts.GossipAddress}
		}
		tcpAddr, _, nsqd := mustStartNSQD(opts)
		defer nsqd.Exit()

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
	firstPort := nsqds[0].tcpListener.Addr().(*net.TCPAddr).Port

	converge(10*time.Second, nsqds, func() bool {
		for _, nsqd := range nsqds {
			if len(nsqd.rdb.FindProducers("topic", topicName, "")) != 1 ||
				len(nsqd.rdb.FindProducers("channel", topicName, "ch")) != 1 {
				return false
			}
		}
		return true
	})

	for _, nsqd := range nsqds {
		producers := nsqd.rdb.FindProducers("topic", topicName, "")
		equal(t, len(producers), 1)
		equal(t, producers[0].TCPPort, firstPort)

		producers = nsqd.rdb.FindProducers("channel", topicName, "ch")
		equal(t, len(producers), 1)
		equal(t, producers[0].TCPPort, firstPort)
	}
}
