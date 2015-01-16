package nsqd

import (
	"encoding/json"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/serf/serf"
	"github.com/shipwire/ansqd/internal/polity"
)

var ExpirationTime time.Duration

type audit struct {
	m *Message
}

type delegate struct {
	n *NSQD
	a *auditor
}

// OnFinish is called when FIN is received for the message
func (d *delegate) OnFinish(m *Message) {
	d.n.GetTopic("audit.finish").PutMessage(&Message{
		ID:   d.n.NewID(),
		Body: m.ID[:],
	})
	log.Printf("AUDIT: OnFinish %x", m.ID)
}

// OnQueue is called before a message is sent to the queue
func (d *delegate) OnQueue(m *Message, topic string) {
	if strings.HasPrefix(topic, "audit.") {
		return
	}
	d.n.GetTopic("audit.send").PutMessage(&Message{
		ID:   d.n.NewID(),
		Body: auditMessage{*m, topic}.Bytes(),
	})
	log.Printf("AUDIT: OnQueue %x", m.ID)
}

// OnRequeue is called when REQ is received for the message
func (d *delegate) OnRequeue(m *Message, delay time.Duration) {
	d.a.Req(m)
}

// OnTouch is called when TOUCH is received for the message
func (d *delegate) OnTouch(m *Message) {
	d.a.Touch(m)
}

type auditor struct {
	p              *polity.Polity
	sendQ, finishQ *nsq.Consumer
	s              *serf.Serf
	n              *NSQD
	hosts          map[string]*Host
	hostsLock      *sync.Mutex
}

func newAuditor(n *NSQD) *auditor {
	id := n.NewID()
	ch := string(id[:]) + "#ephemeral"
	sendQ, _ := nsq.NewConsumer("audit.send", ch, nsq.NewConfig())
	finishQ, _ := nsq.NewConsumer("audit.finish", ch, nsq.NewConfig())
	return &auditor{
		p:         polity.Create(n.serf, n.serfEventChan),
		s:         n.serf,
		n:         n,
		sendQ:     sendQ,
		finishQ:   finishQ,
		hosts:     make(map[string]*Host),
		hostsLock: &sync.Mutex{},
	}
}

func (a auditor) Audit(m *Message) {
	a.ExtractHost(m).AddMessage(*m, time.Now().Add(ExpirationTime))
}

func (a auditor) Fin(m *Message) {
	a.ExtractHost(m).RemoveMessage(*m)
}

func (a auditor) Req(m *Message) {
	a.ExtractHost(m).AddMessage(*m, time.Now().Add(ExpirationTime))
}

func (a auditor) Touch(m *Message) {
	a.ExtractHost(m).AddMessage(*m, time.Now().Add(ExpirationTime))
}

func (a auditor) InitiateRecovery(h *Host) {
	if !h.SetRecovery(true) {
		return
	}

	role := h.RecoveryTopic()
	err := <-a.p.RunElection(role)
	defer a.p.RunRecallElection(role)
	if err != nil {
		return
	}

	for mid, bucket := range h.GetMessages() {
		m := bucket.GetMessage(mid)
		am := extractAudit(m)
		a.n.GetTopic(am.Topic).PutMessage(&am.Message)
	}
}

func (a auditor) ExtractHost(m *Message) *Host {
	body := map[string]interface{}{}
	err := json.Unmarshal(m.Body, &body)
	if err != nil {
		return nil
	}

	var hostname string
	if h, ok := body["hostname"]; !ok {
		return nil
	} else {
		hostname, ok = h.(string)
		if !ok {
			return nil
		}
	}

	return a.GetHost(hostname)
}

func (a auditor) GetHost(hostname string) *Host {
	a.hostsLock.Lock()
	defer a.hostsLock.Unlock()

	host, ok := a.hosts[hostname]
	if !ok {
		host = NewHost(hostname)
		host.InitiateRecovery = func() {
			a.InitiateRecovery(host)
		}
		a.hosts[hostname] = host
	}

	return host
}

type auditMessage struct {
	Message
	Topic string
}

func (a auditMessage) Bytes() []byte {
	return nil
}

func extractAudit(m Message) auditMessage {
	return auditMessage{}
}
