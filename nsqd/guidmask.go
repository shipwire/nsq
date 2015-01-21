package nsqd

import "time"

type guids []guid

type guidFilter interface {
	match(g guid) bool
}

type workerFilter struct {
	id int64
}

func (w workerFilter) match(g guid) bool {
	g = g << workerIdShift
	g = g >> timestampShift
	worker := g >> sequenceBits
	return int64(worker) == w.id
}

type timestampFilter struct {
	earliest, latest time.Time
}

func (t timestampFilter) match(g guid) bool {
	g = g >> timestampShift
	at := time.Unix(0, int64(g))
	return at.Before(t.latest) && at.After(t.earliest)
}

type sequenceFilter struct {
	earliest, latest int64
}

func (s sequenceFilter) match(g guid) bool {
	g = g << timestampShift
	g = g << workerIdShift
	g = g >> workerIdShift
	g = g >> timestampShift
	return int64(g) > s.earliest && int64(g) < s.latest
}

type andFilter []guidFilter

func (fs andFilter) match(g guid) bool {
	for _, f := range fs {
		if !f.match(g) {
			return false
		}
	}
	return true
}

type orFilter []guidFilter

func (fs orFilter) match(g guid) bool {
	for _, f := range fs {
		if f.match(g) {
			return true
		}
	}
	return false
}

func (gs guids) Filter(f guidFilter) guids {
	matched := make(guids, 0)
	for _, g := range gs {
		if f.match(g) {
			matched = append(matched, g)
		}
	}
	return matched
}
