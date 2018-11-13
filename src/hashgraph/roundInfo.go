package hashgraph

import (
	"bytes"
	"fmt"

	"github.com/mosaicnetworks/babble/src/peers"
	"github.com/ugorji/go/codec"
)

type Trilean int

const (
	Undefined Trilean = iota
	True
	False
)

var trileans = []string{"Undefined", "True", "False"}

func (t Trilean) String() string {
	return trileans[t]
}

type pendingRound struct {
	Index   int
	Decided bool
}

type RoundEvent struct {
	RoundReceived int
	Witness       bool
	Famous        Trilean
}
type RoundReceived []string

func (r *RoundReceived) Marshal() ([]byte, error) {
	b := new(bytes.Buffer)
	jh := new(codec.JsonHandle)
	jh.Canonical = true
	enc := codec.NewEncoder(b, jh)

	if err := enc.Encode(r); err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func (r *RoundReceived) Unmarshal(data []byte) error {
	b := bytes.NewBuffer(data)
	jh := new(codec.JsonHandle)
	jh.Canonical = true
	dec := codec.NewDecoder(b, jh)

	return dec.Decode(r)
}

type RoundCreated struct {
	Events map[string]RoundEvent
	// ReceivedEvents []string
	PeerSet *peers.PeerSet
	queued  bool
}

func NewRoundCreated(peers *peers.PeerSet) *RoundCreated {
	return &RoundCreated{
		Events: make(map[string]RoundEvent),
		// ReceivedEvents: []string{},
		PeerSet: peers,
	}
}

func (r *RoundCreated) AddCreatedEvent(x string, witness bool) {
	_, ok := r.Events[x]
	if !ok {
		r.Events[x] = RoundEvent{
			Witness: witness,
		}
	}
}

func (r *RoundCreated) SetRoundReceived(x string, round int) {
	e, ok := r.Events[x]

	if !ok {
		return
	}

	e.RoundReceived = round

	r.Events[x] = e
}

func (r *RoundCreated) SetFame(x string, f bool) {
	e, ok := r.Events[x]
	if !ok {
		e = RoundEvent{
			Witness: true,
		}
	}

	if f {
		e.Famous = True
	} else {
		e.Famous = False
	}

	r.Events[x] = e
}

//return true if no witnesses' fame is left undefined
func (r *RoundCreated) WitnessesDecided() bool {
	c := 0
	for _, e := range r.Events {
		if e.Witness && e.Famous != Undefined {
			c++
		}
	}
	return c >= r.PeerSet.SuperMajority()
}

//return witnesses
func (r *RoundCreated) Witnesses() []string {
	res := []string{}
	for x, e := range r.Events {
		if e.Witness {
			res = append(res, x)
		}
	}

	return res
}

//return famous witnesses
func (r *RoundCreated) FamousWitnesses() []string {
	res := []string{}
	for x, e := range r.Events {
		if e.Witness && e.Famous == True {
			res = append(res, x)
		}
	}
	return res
}

func (r *RoundCreated) IsDecided(witness string) bool {
	w, ok := r.Events[witness]
	return ok && w.Witness && w.Famous != Undefined
}

func (r *RoundCreated) Marshal() ([]byte, error) {
	b := new(bytes.Buffer)
	jh := new(codec.JsonHandle)
	jh.Canonical = true
	enc := codec.NewEncoder(b, jh)

	if err := enc.Encode(r); err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func (r *RoundCreated) Unmarshal(data []byte) error {
	fmt.Println("OUAT THE FUCK", string(data))
	b := bytes.NewBuffer(data)
	jh := new(codec.JsonHandle)
	jh.Canonical = true
	dec := codec.NewDecoder(b, jh)

	return dec.Decode(r)
}

func (r *RoundCreated) IsQueued() bool {
	return r.queued
}
