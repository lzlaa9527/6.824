package shardctrler

import "testing"

func TestHashRing(t *testing.T) {
	hs := NewHashring()

	t.Logf("Join(0)\n")
	if err := hs.Join(0); err != nil {
		t.Fatal(err)
	}

	t.Logf("Move(0..19, 0)\n")
	for i := 0; i < 20; i++ {
		if err := hs.Move(i, 0); err != nil {
			t.Fatal(err)
		}
	}


	t.Logf("Join(1)\n")
	if err := hs.Join(1); err != nil {
		t.Fatal(err)
	}
	hs.String()
	t.Log()

	t.Logf("Move(20..99, 1)\n")
	for i := 20; i < 100; i++ {
		if err := hs.Move(i, 1); err != nil {
			t.Fatal(err)
		}
	}

	t.Logf("Join(2)\n")
	if err := hs.Join(2); err != nil {
		t.Fatal(err)
	}
	hs.String()
	t.Log()

	t.Logf("Join(3)\n")
	if err := hs.Join(3); err != nil {
		t.Fatal(err)
	}
	hs.String()
	t.Log()

	t.Logf("Join(4)\n")
	if err := hs.Join(4); err != nil {
		t.Fatal(err)
	}
	hs.String()
	t.Log()

	t.Logf("Move(20..59, 2)\n")
	for i := 20; i < 60; i++ {
		if err := hs.Move(i, 2); err != nil {
			t.Fatal(err)
		}
	}

	t.Logf("Leave(1)\n")
	if err := hs.Leave(1); err != nil {
		t.Fatal(err)
	}
	hs.String()
	t.Log()
}
