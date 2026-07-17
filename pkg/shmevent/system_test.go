package shmevent

import (
	"bytes"
	"crypto/ed25519"
	"testing"
)

func TestSystemKeyLayout(t *testing.T) {
	key := SystemKey(KindPermitPeer, StatusPending, []byte("peer-123"))
	want := append([]byte{SystemKeyPrefix, KindPermitPeer, StatusPending}, []byte("peer-123")...)
	if !bytes.Equal(key, want) {
		t.Fatalf("got key %x, want %x", key, want)
	}
	if key[0] != 0x00 {
		t.Fatalf("SystemKey's first byte = %#x, want 0x00", key[0])
	}
}

func TestPermitRequestPayloadRoundTrip(t *testing.T) {
	payload, err := EncodePermitRequestPayload(KindBootstrapNode, []byte("peer-123"), []byte("/ip4/1.2.3.4/tcp/4001"))
	if err != nil {
		t.Fatalf("EncodePermitRequestPayload: %v", err)
	}
	kind, peerID, metadata, err := DecodePermitRequestPayload(payload)
	if err != nil {
		t.Fatalf("DecodePermitRequestPayload: %v", err)
	}
	if kind != KindBootstrapNode || string(peerID) != "peer-123" || string(metadata) != "/ip4/1.2.3.4/tcp/4001" {
		t.Fatalf("got kind=%d peerID=%q metadata=%q", kind, peerID, metadata)
	}

	// Empty metadata must round-trip too (the KindPermitPeer common case).
	payload, err = EncodePermitRequestPayload(KindPermitPeer, []byte("peer-456"), nil)
	if err != nil {
		t.Fatalf("EncodePermitRequestPayload with empty metadata: %v", err)
	}
	kind, peerID, metadata, err = DecodePermitRequestPayload(payload)
	if err != nil {
		t.Fatalf("DecodePermitRequestPayload with empty metadata: %v", err)
	}
	if kind != KindPermitPeer || string(peerID) != "peer-456" || len(metadata) != 0 {
		t.Fatalf("got kind=%d peerID=%q metadata=%q, want empty metadata", kind, peerID, metadata)
	}

	if _, _, _, err := DecodePermitRequestPayload([]byte{0, 0}); err == nil {
		t.Fatal("DecodePermitRequestPayload unexpectedly accepted a payload shorter than the header")
	}
	if _, _, _, err := DecodePermitRequestPayload([]byte{KindPermitPeer, 0, 10}); err == nil {
		t.Fatal("DecodePermitRequestPayload unexpectedly accepted a peerID length exceeding the payload size")
	}
}

func TestPermitConfirmPayloadRoundTrip(t *testing.T) {
	payload := EncodePermitConfirmPayload(KindPermitPeer, []byte("peer-123"))
	kind, peerID, err := DecodePermitConfirmPayload(payload)
	if err != nil {
		t.Fatalf("DecodePermitConfirmPayload: %v", err)
	}
	if kind != KindPermitPeer || string(peerID) != "peer-123" {
		t.Fatalf("got kind=%d peerID=%q", kind, peerID)
	}

	if _, _, err := DecodePermitConfirmPayload(nil); err == nil {
		t.Fatal("DecodePermitConfirmPayload unexpectedly accepted an empty payload")
	}
}

func TestEventPermitRequestConfirmEncodeDecodeRoundTrip(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}

	reqPayload, err := EncodePermitRequestPayload(KindPermitPeer, []byte("peer-123"), nil)
	if err != nil {
		t.Fatalf("EncodePermitRequestPayload: %v", err)
	}
	buf, err := Encode(Msg{EventType: EventPermitRequest, Value: reqPayload, ID: 11}, priv)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, _, _, err := Decode(buf)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	kind, peerID, _, err := DecodePermitRequestPayload(got.Value)
	if err != nil {
		t.Fatalf("DecodePermitRequestPayload: %v", err)
	}
	if kind != KindPermitPeer || string(peerID) != "peer-123" {
		t.Fatalf("got kind=%d peerID=%q", kind, peerID)
	}

	confirmPayload := EncodePermitConfirmPayload(KindPermitPeer, []byte("peer-123"))
	buf, err = Encode(Msg{EventType: EventPermitConfirm, Value: confirmPayload, ID: 12}, priv)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, _, _, err = Decode(buf)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	kind, peerID, err = DecodePermitConfirmPayload(got.Value)
	if err != nil {
		t.Fatalf("DecodePermitConfirmPayload: %v", err)
	}
	if kind != KindPermitPeer || string(peerID) != "peer-123" {
		t.Fatalf("got kind=%d peerID=%q", kind, peerID)
	}
}
