package cursor

import (
	"encoding/base64"
	"testing"
)

func TestIDRoundTrip(t *testing.T) {
	for _, id := range []uint64{1, 12345, 1<<53 - 1} {
		got, err := DecodeID(EncodeID(id))
		if err != nil || got != id {
			t.Fatalf("id %d: got %d err %v", id, got, err)
		}
	}
}

func TestDecodeEmpty(t *testing.T) {
	id, err := DecodeID("")
	if err != nil || id != 0 {
		t.Fatalf("empty cursor: got %d err %v", id, err)
	}
	if EncodeID(0) != "" {
		t.Fatal("EncodeID(0) should be empty")
	}
}

func TestDecodeInvalid(t *testing.T) {
	enc := func(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }
	cases := []string{"!!!", "abc", enc("not json"), enc(`{"id":0}`), enc(`{"id":-1}`), enc(`{}`)}
	for _, c := range cases {
		if _, err := DecodeID(c); err == nil {
			t.Fatalf("cursor %q should be invalid", c)
		}
	}
}
