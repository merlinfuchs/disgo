package gateway

import (
	"testing"

	"github.com/disgoorg/json/v2"
)

func TestMessageInvalidDispatch(t *testing.T) {
	data := []byte(`{"op": 0, "s": 42, "t": "CHANNEL_CREATE", "d": {"id": "not a snowflake", "type": 0}}`)

	var message Message
	if err := json.Unmarshal(data, &message); err != nil {
		t.Fatalf("unmarshal message: %v", err)
	}
	if message.S != 42 {
		t.Errorf("got seq %d, want 42", message.S)
	}
	invalid, ok := message.D.(EventInvalid)
	if !ok {
		t.Fatalf("got %T, want EventInvalid", message.D)
	}
	if invalid.Err == nil {
		t.Error("got nil error")
	}
}

func TestMessageInvalidReady(t *testing.T) {
	data := []byte(`{"op": 0, "s": 1, "t": "READY", "d": {"v": "not a number"}}`)

	var message Message
	if err := json.Unmarshal(data, &message); err == nil {
		t.Fatal("got nil error for invalid READY")
	}
}
