package hashgraph

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/mosaicnetworks/babble/src/common"
	"github.com/mosaicnetworks/babble/src/crypto"
	"github.com/mosaicnetworks/babble/src/peers"
)

/*
We introduce a new participant at Round 2, and remove another participant at
round 5.

Round 7
P: [1,2,3]      w71   |    |
         ----------\--------------
Round 6          |   h23   |
P: [1,2,3]       |    | \  |
                 |    |   w63
                 |    | /  |
				 |  / |    |
                w61   |    |
                 | \  |    |
                 |   w62   |
		 ----------------\--------
Round 5          |    |   j31
P: [1,2,3]       |    | /  |
				 |  / |    |
                w51   |    |
                 | \  |    |
                 |   w52   |
                 |    | \  |
		         |    |   w53
         ---------------/---------
Round 4          |   w42   |
P:[0,1,2,3]      | /  |    |
                w41   |    |
               / |    |    |
      		w40  |    |    |
            |    \    |    |
            |    |    \    |
		    |    |    |   w43
         ---------------/---------
Round 3     |    |   w32   |
P:[0,1,2,3] |    | /  |    |
            |   w31   |    |
            |  / |    |    |
            w30  |    |    |
            |    \    |    |
            |    |    \    |
		    |    |    |   w33
         -------------------------
Round 2		|    |    | /  |
P:[0,1,2,3] |    |   g21   R3
			|    | /  |
			|   w21   |
			| /  |    |
		   w20   |    |
		    |  \ |    |
		    |    | \  |
		    |    |   w22
		 -----------/------
Round 1		|   f10   |
P:[0,1,2]	| /  |    |
		   w10   |    |
		    |  \ |    |
		    |    | \  |
		    |    |   w12
		    |    |  / |
		    |   w11   |
		 -----/------------
Round 0	   e12   |    |
P:[0,1,2]   |  \ |    |
		    |    | \  |
		    |    |   e21
		    |    | /  |
		    |   e10   |
		    |  / |    |
		   w00  w01  w02
			|    |    |
		    R0   R1   R2
			0	 1	  2
*/
func initR2DynHashgraph(t testing.TB) (*Hashgraph, map[string]string) {
	nodes, index, orderedEvents, peerSet := initHashgraphNodes(3)

	for i, peer := range peerSet.Peers {
		name := fmt.Sprintf("w0%d", i)
		event := NewEvent([][]byte{[]byte(name)}, nil, nil, []string{rootSelfParent(peer.ID), ""}, nodes[i].Pub, 0)
		nodes[i].signAndAddEvent(event, name, index, orderedEvents)
	}

	plays := []play{
		play{1, 1, "w01", "w00", "e10", [][]byte{[]byte("e10")}, nil},
		play{2, 1, "w02", "e10", "e21", [][]byte{[]byte("e21")}, nil},
		play{0, 1, "w00", "e21", "e12", [][]byte{[]byte("e12")}, nil},
		play{1, 2, "e10", "e12", "w11", [][]byte{[]byte("w11")}, nil},
		play{2, 2, "e21", "w11", "w12", [][]byte{[]byte("w12")}, nil},
		play{0, 2, "e12", "w12", "w10", [][]byte{[]byte("w10")}, nil},
		play{1, 3, "w11", "w10", "f10", [][]byte{[]byte("f10")}, nil},
		play{2, 3, "w12", "f10", "w22", [][]byte{[]byte("w22")}, nil},
		play{0, 3, "w10", "w22", "w20", [][]byte{[]byte("w20")}, nil},
		play{1, 4, "f10", "w20", "w21", [][]byte{[]byte("w21")}, nil},
		play{2, 4, "w22", "w21", "g21", [][]byte{[]byte("g21")}, nil},
	}

	playEvents(plays, nodes, index, orderedEvents)

	hg := createHashgraph(false, orderedEvents, peerSet, common.NewTestLogger(t).WithField("test", "R2D"))

	/***************************************************************************
		Add Participant 3; new Peerset for Round2
	***************************************************************************/

	//create new node
	key3, _ := crypto.GenerateECDSAKey()
	node3 := NewTestNode(key3)
	nodes = append(nodes, node3)
	peer3 := peers.NewPeer(node3.PubHex, "")
	index["R3"] = rootSelfParent(peer3.ID)
	newPeerSet := peerSet.WithNewPeer(peer3)

	//Set Round 2 PeerSet
	err := hg.Store.SetPeerSet(2, newPeerSet)
	if err != nil {
		t.Fatal(err)
	}

	/***************************************************************************
		Continue inserting Events with new participant
	***************************************************************************/

	plays = []play{
		play{3, 0, "R3", "g21", "w33", [][]byte{[]byte("w33")}, nil},
		play{0, 4, "w20", "w33", "w30", [][]byte{[]byte("w30")}, nil},
		play{1, 5, "w21", "w30", "w31", [][]byte{[]byte("w31")}, nil},
		play{2, 5, "g21", "w31", "w32", [][]byte{[]byte("w32")}, nil},
		play{3, 1, "w33", "w32", "w43", [][]byte{[]byte("w43")}, nil},
		play{0, 5, "w30", "w43", "w40", [][]byte{[]byte("w40")}, nil},
		play{1, 6, "w31", "w40", "w41", [][]byte{[]byte("w41")}, nil},
		play{2, 6, "w32", "w41", "w42", [][]byte{[]byte("w42")}, nil},
	}

	orderedEvents = &[]*Event{}

	playEvents(plays, nodes, index, orderedEvents)

	for i, ev := range *orderedEvents {
		if err := hg.InsertEvent(ev, true); err != nil {
			t.Fatalf("ERROR inserting event %d: %s\n", i, err)
		}
	}

	/***************************************************************************
		Remove Participant 0; new Peerset for Round5
	***************************************************************************/

	newPeerSet2 := newPeerSet.WithRemovedPeer(newPeerSet.Peers[0])

	//Set Round 5 PeerSet
	err = hg.Store.SetPeerSet(5, newPeerSet2)
	if err != nil {
		t.Fatal(err)
	}

	/***************************************************************************
		Continue inserting Events with new participant
	***************************************************************************/

	plays = []play{
		play{3, 2, "w43", "w42", "w53", [][]byte{[]byte("w53")}, nil},
		play{2, 7, "w42", "w53", "w52", [][]byte{[]byte("w52")}, nil},
		play{1, 7, "w41", "w52", "w51", [][]byte{[]byte("w51")}, nil},
		play{3, 3, "w53", "w51", "j31", [][]byte{[]byte("j31")}, nil},
		play{2, 8, "w52", "j31", "w62", [][]byte{[]byte("w62")}, nil},
		play{1, 8, "w51", "w62", "w61", [][]byte{[]byte("w61")}, nil},
		play{3, 4, "j31", "w61", "w63", [][]byte{[]byte("w63")}, nil},
		play{2, 9, "w62", "w63", "h23", [][]byte{[]byte("h23")}, nil},
		play{1, 9, "w61", "h23", "w71", [][]byte{[]byte("w71")}, nil},
	}

	orderedEvents = &[]*Event{}

	playEvents(plays, nodes, index, orderedEvents)

	for i, ev := range *orderedEvents {
		if err := hg.InsertEvent(ev, true); err != nil {
			t.Fatalf("ERROR inserting event %d: %s\n", i, err)
		}
	}

	return hg, index
}

func TestR2DynDivideRounds(t *testing.T) {
	h, index := initR2DynHashgraph(t)

	if err := h.DivideRounds(); err != nil {
		t.Fatal(err)
	}

	/**************************************************************************/

	//[event] => {lamportTimestamp, round}
	type tr struct {
		t, r int
	}
	expectedTimestamps := map[string]tr{
		"w00": tr{0, 0},
		"w01": tr{0, 0},
		"w02": tr{0, 0},
		"e10": tr{1, 0},
		"e21": tr{2, 0},
		"e12": tr{3, 0},
		"w11": tr{4, 1},
		"w12": tr{5, 1},
		"w10": tr{6, 1},
		"f10": tr{7, 1},
		"w22": tr{8, 2},
		"w20": tr{9, 2},
		"w21": tr{10, 2},
		"g21": tr{11, 2},
		"w33": tr{12, 3},
		"w30": tr{13, 3},
		"w31": tr{14, 3},
		"w32": tr{15, 3},
		"w43": tr{16, 4},
		"w40": tr{17, 4},
		"w41": tr{18, 4},
		"w42": tr{19, 4},
		"w53": tr{20, 5},
		"w52": tr{21, 5},
		"w51": tr{22, 5},
		"j31": tr{23, 5},
		"w62": tr{24, 6},
		"w61": tr{25, 6},
		"w63": tr{26, 6},
		"h23": tr{27, 6},
		"w71": tr{28, 7},
	}

	for e, et := range expectedTimestamps {
		ev, err := h.Store.GetEvent(index[e])
		if err != nil {
			t.Fatal(err)
		}
		if r := ev.round; r == nil || *r != et.r {
			t.Fatalf("%s round should be %d, not %d", e, et.r, *r)
		}
		if ts := ev.lamportTimestamp; ts == nil || *ts != et.t {
			t.Fatalf("%s lamportTimestamp should be %d, not %d", e, et.t, *ts)
		}
	}

	/**************************************************************************/

	expectedWitnesses := map[int][]string{
		0: []string{"w00", "w01", "w02"},
		1: []string{"w10", "w11", "w12"},
		2: []string{"w20", "w21", "w22"},
		3: []string{"w30", "w31", "w32", "w33"},
		4: []string{"w40", "w41", "w42", "w43"},
		5: []string{"w51", "w52", "w53"},
		6: []string{"w61", "w62", "w63"},
		7: []string{"w71"},
	}

	for i := 0; i < 8; i++ {
		round, err := h.Store.GetRoundCreated(i)
		if err != nil {
			t.Fatal(err)
		}
		if l := len(round.Witnesses()); l != len(expectedWitnesses[i]) {
			t.Fatalf("round %d should have %d witnesses, not %d", i, len(expectedWitnesses[i]), l)
		}
		for _, w := range expectedWitnesses[i] {
			if !contains(round.Witnesses(), index[w]) {
				t.Fatalf("round %d witnesses should contain %s", i, w)
			}
		}
	}
}

func TestR2DynDecideFame(t *testing.T) {
	h, index := initR2DynHashgraph(t)

	h.DivideRounds()
	if err := h.DecideFame(); err != nil {
		t.Fatal(err)
	}

	expectedEvents := map[int]map[string]RoundEvent{
		0: map[string]RoundEvent{
			"w00": RoundEvent{Witness: true, Famous: True},
			"w01": RoundEvent{Witness: true, Famous: True},
			"w02": RoundEvent{Witness: true, Famous: True},
			"e10": RoundEvent{Witness: false, Famous: Undefined},
			"e21": RoundEvent{Witness: false, Famous: Undefined},
			"e12": RoundEvent{Witness: false, Famous: Undefined},
		},
		1: map[string]RoundEvent{
			"w10": RoundEvent{Witness: true, Famous: True},
			"w11": RoundEvent{Witness: true, Famous: True},
			"w12": RoundEvent{Witness: true, Famous: True},
			"f10": RoundEvent{Witness: false, Famous: Undefined},
		},
		2: map[string]RoundEvent{
			"w20": RoundEvent{Witness: true, Famous: True},
			"w21": RoundEvent{Witness: true, Famous: True},
			"w22": RoundEvent{Witness: true, Famous: True},
			"g21": RoundEvent{Witness: false, Famous: Undefined},
		},
		3: map[string]RoundEvent{
			"w30": RoundEvent{Witness: true, Famous: True},
			"w31": RoundEvent{Witness: true, Famous: True},
			"w32": RoundEvent{Witness: true, Famous: True},
			"w33": RoundEvent{Witness: true, Famous: True},
		},
		4: map[string]RoundEvent{
			"w40": RoundEvent{Witness: true, Famous: True},
			"w41": RoundEvent{Witness: true, Famous: True},
			"w42": RoundEvent{Witness: true, Famous: True},
			"w43": RoundEvent{Witness: true, Famous: True},
		},
		5: map[string]RoundEvent{
			"w51": RoundEvent{Witness: true, Famous: True},
			"w52": RoundEvent{Witness: true, Famous: True},
			"w53": RoundEvent{Witness: true, Famous: True},
			"j31": RoundEvent{Witness: false, Famous: Undefined},
		},
		6: map[string]RoundEvent{
			"w61": RoundEvent{Witness: true, Famous: Undefined},
			"w62": RoundEvent{Witness: true, Famous: Undefined},
			"w63": RoundEvent{Witness: true, Famous: Undefined},
			"h23": RoundEvent{Witness: false, Famous: Undefined},
		},
		7: map[string]RoundEvent{
			//created
			"w71": RoundEvent{Witness: true, Famous: Undefined},
		},
	}

	for i := 0; i < 8; i++ {
		round, err := h.Store.GetRoundCreated(i)
		if err != nil {
			t.Fatal(err)
		}
		if l := len(round.Events); l != len(expectedEvents[i]) {
			t.Fatalf("Round[%d].Events should contain %d items, not %d", i, len(expectedEvents[i]), l)
		}
		for w, re := range expectedEvents[i] {
			if f := round.Events[index[w]]; !reflect.DeepEqual(f, re) {
				t.Fatalf("%s should be %v; got %v", w, re, f)
			}
		}
	}

}

func TestR2DynDecideRoundReceived(t *testing.T) {
	h, index := initR2DynHashgraph(t)

	h.DivideRounds()
	h.DecideFame()
	if err := h.DecideRoundReceived(); err != nil {
		t.Fatal(err)
	}

	expectedConsensusEvents := map[int]RoundReceived{
		1: RoundReceived{index["w00"], index["w01"], index["w02"], index["e10"], index["e21"], index["e12"]},
		2: RoundReceived{index["w11"], index["w12"], index["w10"], index["f10"]},
		3: RoundReceived{index["w22"], index["w20"], index["w21"], index["g21"]},
		4: RoundReceived{index["w33"], index["w30"], index["w31"], index["w32"]},
		5: RoundReceived{index["w43"], index["w40"], index["w41"], index["w42"]},
	}

	for i := 1; i < 6; i++ {
		round, err := h.Store.GetRoundReceived(i)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(*round, expectedConsensusEvents[i]) {
			t.Fatalf("Round[%d].ReceivedEvents should be %v, %v", i, expectedConsensusEvents[i], *round)
		}
	}

}

func TestR2DynProcessDecidedRounds(t *testing.T) {
	h, index := initR2DynHashgraph(t)

	h.DivideRounds()
	h.DecideFame()
	h.DecideRoundReceived()
	if err := h.ProcessDecidedRounds(); err != nil {
		t.Fatal(err)
	}

	//--------------------------------------------------------------------------
	consensusEvents := h.Store.ConsensusEvents()

	for i, e := range consensusEvents {
		t.Logf("consensus[%d]: %s\n", i, getName(index, e))
	}

	if l := len(consensusEvents); l != 22 {
		t.Fatalf("length of consensus should be 22 not %d", l)
	}

	if ple := h.PendingLoadedEvents; ple != 9 {
		t.Fatalf("PendingLoadedEvents should be 9, not %d", ple)
	}

	//--------------------------------------------------------------------------

	for i := 0; i < 4; i++ {
		rr := i + 1

		frame, err := h.Store.GetFrame(rr)
		if err != nil {
			t.Fatal(err)
		}
		frameHash, _ := frame.Hash()

		ps, err := h.Store.GetPeerSet(rr)
		if err != nil {
			t.Fatal(err)
		}
		peersHash, _ := ps.Hash()

		block, err := h.Store.GetBlock(i)
		if err != nil {
			t.Fatal(err)
		}

		if brr := block.RoundReceived(); brr != rr {
			t.Fatalf("Block[%d].RoundReceived should be %d, not %d", i, rr, brr)
		}

		if bfh := block.FrameHash(); !reflect.DeepEqual(bfh, frameHash) {
			t.Fatalf("Block[%d].FrameHash should be %v, not %v", i, frameHash, bfh)
		}

		if bph := block.PeersHash(); !reflect.DeepEqual(bph, peersHash) {
			t.Fatalf("Block[%d].PeersHash should be %v, not %v", i, peersHash, bph)
		}
	}
}
