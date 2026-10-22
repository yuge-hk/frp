// Copyright 2026 The frp Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !frps

package proxy

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/fatedier/frp/pkg/msg"
	"github.com/fatedier/frp/pkg/util/xlog"
)

// TestRaceUDPProxyInWorkConn drives concurrent work connections through the
// real UDPProxy.InWorkConn path. Each call closes the previous generation of
// readCh/sendCh and installs new ones under pxy.mu, but the channels are read
// again without the lock when forwarding starts, which races with the next
// replacement.
func TestRaceUDPProxyInWorkConn(t *testing.T) {
	cfg := &v1.UDPProxyConfig{
		ProxyBaseConfig: v1.ProxyBaseConfig{
			Name: "udp-race",
			Type: "udp",
			ProxyBackend: v1.ProxyBackend{
				LocalIP:   "127.0.0.1",
				LocalPort: 9,
			},
		},
	}
	base := &BaseProxy{
		baseCfg:   &cfg.ProxyBaseConfig,
		clientCfg: &v1.ClientCommonConfig{UDPPacketSize: 1500},
		xl:        xlog.New(),
		ctx:       context.Background(),
	}
	pxy := NewUDPProxy(base, cfg).(*UDPProxy)
	require.NoError(t, pxy.Run())
	defer pxy.Close()

	const workers = 64
	peers := make([]net.Conn, 0, workers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		client, server := net.Pipe()
		peers = append(peers, server)
		go func() {
			defer wg.Done()
			<-start
			pxy.InWorkConn(client, &msg.StartWorkConn{})
		}()
	}
	close(start)

	// InWorkConn blocks in udp.Forwarder for the lifetime of the work
	// connection, so give the race detector time to observe the overlapping
	// field reads and writes before tearing the test down.
	time.Sleep(2 * time.Second)
	for _, peer := range peers {
		_ = peer.Close()
	}
}
