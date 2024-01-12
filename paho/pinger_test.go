/*
 * Copyright (c) 2024 Contributors to the Eclipse Foundation
 *
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v2.0
 *  and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 * The Eclipse Public License is available at
 *    https://www.eclipse.org/legal/epl-2.0/
 *  and the Eclipse Distribution License is available at
 *    http://www.eclipse.org/org/documents/edl-v10.php.
 *
 *  SPDX-License-Identifier: EPL-2.0 OR BSD-3-Clause
 */

package paho

import (
	"net"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/packets"
	paholog "github.com/eclipse/paho.golang/paho/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultPingerTimeout(t *testing.T) {
	fakeServerConn, fakeClientConn := net.Pipe()

	go func() {
		// keep reading from fakeServerConn and throw away the data
		buf := make([]byte, 1024)
		for {
			_, err := fakeServerConn.Read(buf)
			if err != nil {
				return
			}
		}
	}()

	pinger := NewDefaultPinger()
	pinger.SetDebug(paholog.NewTestLogger(t, "DefaultPinger:"))

	pingResult := make(chan error, 1)
	go func() {
		pingResult <- pinger.Run(fakeClientConn, 1)
	}()
	defer pinger.Stop()

	select {
	case err := <-pingResult:
		require.NotNil(t, err)
		assert.EqualError(t, err, "PINGRESP timed out")
	case <-time.After(10 * time.Second):
		t.Error("expected DefaultPinger to detect timeout and return error")
	}
}

func TestDefaultPingerSuccess(t *testing.T) {
	fakeClientConn, fakeServerConn := net.Pipe()

	pinger := NewDefaultPinger()
	pinger.SetDebug(paholog.NewTestLogger(t, "DefaultPinger:"))

	pingResult := make(chan error, 1)
	go func() {
		pingResult <- pinger.Run(fakeClientConn, 3)
	}()
	defer pinger.Stop()

	go func() {
		// keep reading from fakeServerConn and call PingResp() when a PINGREQ is received
		for {
			recv, err := packets.ReadPacket(fakeServerConn)
			if err != nil {
				return
			}
			if recv.Type == packets.PINGREQ {
				pinger.PingResp()
			}
		}
	}()

	select {
	case err := <-pingResult:
		t.Errorf("expected DefaultPinger to not return error, got %v", err)
	case <-time.After(10 * time.Second):
		// PASS
	}
}
