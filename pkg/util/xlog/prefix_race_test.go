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

package xlog

import (
	"fmt"
	"sync"
	"testing"
)

// TestRacePrefixAddAndLog reproduces the unsynchronized access to Logger
// prefixes and the rendered prefix string. It uses the same operations as the
// real client paths: client/service.go updates the shared logger with
// AddPrefix when a login returns a run ID, while proxy goroutines call Spawn
// and Tracef on the same logger.
func TestRacePrefixAddAndLog(t *testing.T) {
	xl := New().AppendPrefix("frpc")

	const iterations = 20000
	start := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(3)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < iterations; i++ {
			xl.AddPrefix(LogPrefix{Name: "runID", Value: fmt.Sprintf("run-%d", i)})
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < iterations; i++ {
			xl.Tracef("trace %d", i)
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < iterations; i++ {
			xl.Spawn().Tracef("spawned %d", i)
		}
	}()

	close(start)
	wg.Wait()
}
