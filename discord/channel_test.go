package discord

import (
	"testing"

	"github.com/disgoorg/json/v2"
	"github.com/disgoorg/snowflake/v2"
)

func TestGatewayGuildUnknownChannelType(t *testing.T) {
	data := []byte(`{
		"id": "1",
		"name": "guild",
		"channels": [
			{"id": "2", "type": 0, "name": "general", "position": 0},
			{"id": "3", "type": 20, "name": "new", "position": 1, "parent_id": "4", "permission_overwrites": [{"id": "1", "type": 0, "allow": "0", "deny": "1024"}]}
		]
	}`)

	var guild GatewayGuild
	if err := json.Unmarshal(data, &guild); err != nil {
		t.Fatalf("unmarshal guild: %v", err)
	}
	if len(guild.Channels) != 2 {
		t.Fatalf("got %d channels, want 2", len(guild.Channels))
	}

	c, ok := guild.Channels[1].(UnknownChannel)
	if !ok {
		t.Fatalf("got %T, want UnknownChannel", guild.Channels[1])
	}
	if c.ID() != 3 || c.Type() != 20 || c.Name() != "new" || c.Position() != 1 {
		t.Errorf("got %s at position %d", c, c.Position())
	}
	if c.ParentID() == nil || *c.ParentID() != 4 {
		t.Errorf("got parent %v, want 4", c.ParentID())
	}
	if len(c.PermissionOverwrites()) != 1 {
		t.Errorf("got %d overwrites, want 1", len(c.PermissionOverwrites()))
	}

	c = ApplyGuildIDToChannel(c, 1).(UnknownChannel)
	if c.GuildID() != snowflake.ID(1) {
		t.Errorf("got guild %d, want 1", c.GuildID())
	}
}
