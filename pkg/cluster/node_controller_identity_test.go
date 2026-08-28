package cluster

import "testing"

func TestDefaultControlRuntimeAddr(t *testing.T) {
	for _, tt := range []struct {
		name string
		cfg  Config
		want string
	}{
		{
			name: "seed join canonical advertisement",
			cfg: Config{NodeID: 2, ListenAddr: "0.0.0.0:7001",
				Join: JoinConfig{Seeds: []string{"node1:7001"}, AdvertiseAddr: " node2:7001 "}},
			want: "node2:7001",
		},
		{
			name: "static local voter membership",
			cfg: Config{NodeID: 2, ListenAddr: "0.0.0.0:7001",
				Control: ControlConfig{Voters: []ControlVoter{{NodeID: 1, Addr: "node1:7001"}, {NodeID: 2, Addr: "node2:7001"}}}},
			want: "node2:7001",
		},
		{
			name: "implicit single node cluster",
			cfg:  Config{NodeID: 1, ListenAddr: "127.0.0.1:7001"},
			want: "127.0.0.1:7001",
		},
		{
			name: "static mirror without local voter entry",
			cfg: Config{NodeID: 2, ListenAddr: "node2:7001",
				Control: ControlConfig{Role: ControlRoleMirror, Voters: []ControlVoter{{NodeID: 1, Addr: "node1:7001"}}}},
			want: "node2:7001",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tt.cfg.applyControlDefaults()
			n := &Node{cfg: tt.cfg}
			if got := n.defaultControlRuntimeAddr(); got != tt.want {
				t.Fatalf("defaultControlRuntimeAddr() = %q, want %q", got, tt.want)
			}
		})
	}
}
