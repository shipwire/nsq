package nsqd

import (
	"net"
	"sort"
	"testing"
	"time"
)

type gossipTester struct {
	c chan struct{}
}

func (g gossipTester) notify() {
	g.c <- struct{}{}
	select {
	case g.c <- struct{}{}:
	default:
	}
}

func TestGossip(t *testing.T) {
	var nsqds []*NSQD
	var seedNode *NSQD
	var tcpPorts []int

	convergenceTester := gossipTester{make(chan struct{}, 20)}

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
			opts.GossipSeedAddresses = []string{seedNode.opts.GossipAddress}
		}
		tcpAddr, _, nsqd := mustStartNSQD(opts)
		defer nsqd.Exit()

		nsqds = append(nsqds, nsqd)
		tcpPorts = append(tcpPorts, tcpAddr.Port)

		if seedNode == nil {
			seedNode = nsqd
		}
	}
	// sort the ports for later comparison
	sort.Ints(tcpPorts)

	// wait for convergence
	converge(5*time.Second, nsqds, convergenceTester.c, func() bool {
		for _, nsqd := range nsqds {
			if len(nsqd.rdb.FindProducers("client", "", "")) != num {
				return false
			}
		}
		return true
	})

	// all nodes in the cluster should have registrations
	for _, nsqd := range nsqds {
		producers := nsqd.rdb.FindProducers("client", "", "")
		var actTCPPorts []int
		for _, producer := range producers {
			actTCPPorts = append(actTCPPorts, producer.TCPPort)
		}
		sort.Ints(actTCPPorts)

		equal(t, len(producers), num)
		equal(t, tcpPorts, actTCPPorts)
	}

	// create a topic/channel on the first node
	topicName := "topic1"
	topic := nsqds[0].GetTopic(topicName)
	topic.GetChannel("ch")
	firstPort := nsqds[0].tcpListener.Addr().(*net.TCPAddr).Port

	converge(10*time.Second, nsqds, convergenceTester.c, func() bool {
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

func TestGossipResync(t *testing.T) {
	var nsqds []*NSQD
	var seedNode *NSQD
	var tcpPorts []int

	convergenceTester := gossipTester{make(chan struct{}, 20)}

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
			opts.GossipSeedAddresses = []string{seedNode.opts.GossipAddress}
		}
		tcpAddr, _, nsqd := mustStartNSQD(opts)
		if i < num-1 { // we'll close the last one manually
			defer nsqd.Exit()
		}

		nsqds = append(nsqds, nsqd)
		tcpPorts = append(tcpPorts, tcpAddr.Port)

		if seedNode == nil {
			seedNode = nsqd
		}
	}

	// create a topic/channel on the first node
	topicName := "topic1"
	topic := nsqds[0].GetTopic(topicName)
	topic.GetChannel("ch")
	firstPort := nsqds[0].tcpListener.Addr().(*net.TCPAddr).Port

	converge(10*time.Second, nsqds, convergenceTester.c, func() bool {
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

	// test re-gossiping; close a node
	nsqds[num-1].Exit()
	stillAlive := nsqds[:num-1]

	// check that other nodes see it as closed
	converge(10*time.Second, stillAlive, convergenceTester.c, func() bool {
		for _, nsqd := range stillAlive {
			if len(nsqd.serf.Members()) != len(stillAlive) {
				return false
			}
		}
		return true
	})

	// restart stopped node
	_, _, nsqd := mustStartNSQD(nsqds[num-1].opts)
	defer nsqd.Exit()
	nsqds[num-1] = nsqd

	// check that other nodes see it as back open
	converge(10*time.Second, nsqds, convergenceTester.c, func() bool {
		for _, nsqd := range nsqds {
			if len(nsqd.serf.Members()) != len(nsqds) {
				return false
			}
		}
		return true
	})

	// check that all nodes see the restarted first node
	converge(10*time.Second, nsqds, convergenceTester.c, func() bool {
		for _, nsqd := range nsqds {
			if len(nsqd.rdb.FindProducers("topic", topicName, "")) != 1 ||
				len(nsqd.rdb.FindProducers("channel", topicName, "ch")) != 1 {
				return false
			}
		}
		return true
	})

	// we should have producers for the topic/channel back now
	for _, nsqd := range nsqds {
		producers := nsqd.rdb.FindProducers("topic", topicName, "")
		equal(t, len(producers), 1)
		equal(t, producers[0].TCPPort, firstPort)

		producers = nsqd.rdb.FindProducers("channel", topicName, "ch")
		equal(t, len(producers), 1)
		equal(t, producers[0].TCPPort, firstPort)
	}
}

func TestRegossip(t *testing.T) {
	var nsqds []*NSQD
	var seedNode *NSQD
	var tcpPorts []int

	convergenceTester := gossipTester{make(chan struct{}, 20)}

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
		opts.GossipRegossipInterval = 5 * time.Second
		opts.gossipDelegate = convergenceTester
		if seedNode != nil {
			opts.GossipSeedAddresses = []string{seedNode.opts.GossipAddress}
		}
		tcpAddr, _, nsqd := mustStartNSQD(opts)
		if i < num-1 { // we'll close the last one manually
			defer nsqd.Exit()
		}

		nsqds = append(nsqds, nsqd)
		tcpPorts = append(tcpPorts, tcpAddr.Port)

		if seedNode == nil {
			seedNode = nsqd
		}
	}

	// create a topic/channel on the first node
	topicName := "topic1"
	topic := nsqds[0].GetTopic(topicName)
	topic.GetChannel("ch")
	firstPort := nsqds[0].tcpListener.Addr().(*net.TCPAddr).Port

	converge(10*time.Second, nsqds, convergenceTester.c, func() bool {
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

	// test re-gossiping; delete info on one node
	regs := nsqds[num-1].rdb.FindRegistrations("topic", topicName, "")
	regs = append(regs, nsqds[num-1].rdb.FindRegistrations("channel", topicName, "ch")...)
	for _, reg := range regs {
		nsqds[num-1].rdb.RemoveRegistration(reg)
	}

	// wait for regossip
	converge(10*time.Second, nsqds, convergenceTester.c, func() bool {
		for _, nsqd := range nsqds {
			if len(nsqd.rdb.FindProducers("topic", topicName, "")) != 1 ||
				len(nsqd.rdb.FindProducers("channel", topicName, "ch")) != 1 {
				return false
			}
		}
		return true
	})

	// we should have producers for the topic/channel back now on all nodes
	for _, nsqd := range nsqds {
		producers := nsqd.rdb.FindProducers("topic", topicName, "")
		equal(t, len(producers), 1)
		equal(t, producers[0].TCPPort, firstPort)

		producers = nsqd.rdb.FindProducers("channel", topicName, "ch")
		equal(t, len(producers), 1)
		equal(t, producers[0].TCPPort, firstPort)
	}
}

func converge(timeout time.Duration, nsqds []*NSQD, notifyChan chan struct{}, isConverged func() bool) {
	// wait for convergence
	converged := false
	t := time.NewTimer(timeout)
	for !converged {
		select {
		case <-t.C:
			converged = true
		case <-notifyChan:
			converged = isConverged()
		}
	}
}
