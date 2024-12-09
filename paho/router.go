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
	"strings"
	"sync"

	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho/log"
)

// MessageHandler is a type for a function that is invoked
// by a Router when it has received a Publish.
// MessageHandlers should complete quickly (start a go routine for
// long-running processes) and should not call functions within the
// paho instance that triggered them (due to potential deadlocks).
type MessageHandler func(*Publish)

// Router is an interface of the functions for a struct that is
// used to handle invoking MessageHandlers depending on the
// the topic the message was published on.
// RegisterHandler() takes a string of the topic, and a MessageHandler
// to be invoked when Publishes are received that match that topic
// UnregisterHandler() takes a string of the topic to remove
// MessageHandlers for
// Route() takes a Publish message and determines which MessageHandlers
// should be invoked
type Router interface {
	RegisterHandler(string, MessageHandler)
	UnregisterHandler(string)
	Route(*packets.Publish)
	SetDebugLogger(log.Logger)
}

// StandardRouter is a library provided implementation of a Router that
// allows for unique and multiple MessageHandlers per topic
type StandardRouter struct {
	sync.RWMutex
	defaultHandler MessageHandler
	subscriptions  map[string][]MessageHandler
	aliases        map[uint16]string
	debug          log.Logger
}

// NewStandardRouter instantiates and returns an instance of a StandardRouter
func NewStandardRouter() *StandardRouter {
	return &StandardRouter{
		subscriptions: make(map[string][]MessageHandler),
		aliases:       make(map[uint16]string),
		debug:         log.NOOPLogger{},
	}
}

// NewStandardRouterWithDefault instantiates and returns an instance of a StandardRouter
// with the default handler set to the value passed in (for convenience when creating
// handler inline).
func NewStandardRouterWithDefault(h MessageHandler) *StandardRouter {
	r := NewStandardRouter()
	r.DefaultHandler(h)
	return r
}

// RegisterHandler is the library provided StandardRouter's
// implementation of the required interface function()
func (r *StandardRouter) RegisterHandler(topic string, h MessageHandler) {
	r.debug.Println("registering handler for:", topic)
	r.Lock()
	defer r.Unlock()

	r.subscriptions[topic] = append(r.subscriptions[topic], h)
}

// UnregisterHandler is the library provided StandardRouter's
// implementation of the required interface function()
func (r *StandardRouter) UnregisterHandler(topic string) {
	r.debug.Println("unregistering handler for:", topic)
	r.Lock()
	defer r.Unlock()

	delete(r.subscriptions, topic)
}

// Route is the library provided StandardRouter's implementation
// of the required interface function()
func (r *StandardRouter) Route(pb *packets.Publish) {
	r.debug.Println("routing message for:", pb.Topic)
	r.RLock()
	defer r.RUnlock()

	m := PublishFromPacketPublish(pb)

	var topic string
	if pb.Properties.TopicAlias != nil {
		r.debug.Println("message is using topic aliasing")
		if pb.Topic != "" {
			// Register new alias
			r.debug.Printf("registering new topic alias '%d' for topic '%s'", *pb.Properties.TopicAlias, m.Topic)
			r.aliases[*pb.Properties.TopicAlias] = pb.Topic
		}
		if t, ok := r.aliases[*pb.Properties.TopicAlias]; ok {
			r.debug.Printf("aliased topic '%d' translates to '%s'", *pb.Properties.TopicAlias, m.Topic)
			topic = t
		}
	} else {
		topic = m.Topic
	}

	handlerCalled := false
	for route, handlers := range r.subscriptions {
		if route == topic || match(route, topic) { 
			r.debug.Println("found handler for:", route)
			for _, handler := range handlers {
				handler(m)
				handlerCalled = true
			}
		}
	}

	if !handlerCalled && r.defaultHandler != nil {
		r.defaultHandler(m)
	}
}

// SetDebugLogger sets the logger l to be used for printing debug
// information for the router
func (r *StandardRouter) SetDebugLogger(l log.Logger) {
	r.debug = l
}

// DefaultHandler sets handler to be called for messages that don't trigger another handler
// Pass nil to unset.
func (r *StandardRouter) DefaultHandler(h MessageHandler) {
	r.debug.Println("registering default handler")
	r.Lock()
	defer r.Unlock()
	r.defaultHandler = h
}

func match(route, topic string) bool {
	s1 := strings.Split(route, "/")
	s2 := strings.Split(topic, "/")
	s1p := 0
	s2p := 0
	for s1p < len(s1) {
		if s2p < len(s2) && s1[s1p] != "#" {
			if s1[s1p] == s2[s2p] || s1[s1p] == "+" {
				s1p++
				s2p++
				continue
			} else {
				return false
			}
		} else if s1[s1p] != "#" && s2p >= len(s2) {
			return false
		}
		nPlusSign := 0
	NoWildcard:
		for s1p++; s1p < len(s1); s1p++ {
			switch s1[s1p] {
			case "#":
				continue
			case "+":
				nPlusSign++
				continue
			default:
				break NoWildcard
			}
		}
		s2p = s2p + nPlusSign
		if s2p > len(s2) {
			return false
		}
		if s1p >= len(s1) {
			return true
		}
		for ; s2p < len(s2); s2p++ {
			if s1[s1p] == s2[s2p] {
				s1p++
				s2p++
				break
			}
		}
	}
	if s2p != len(s2) {
		return false
	}
	return true
}

func routeSplit(route string) []string {
	if len(route) == 0 {
		return nil
	}
	var result []string
	if strings.HasPrefix(route, "$share") {
		result = strings.Split(route, "/")[2:]
	} else {
		result = strings.Split(route, "/")
	}
	return result
}

func topicSplit(topic string) []string {
	if len(topic) == 0 {
		return nil
	}
	return strings.Split(topic, "/")
}

// NewSingleHandlerRouter instantiates a router that will call the passed in message handler for all
// inbound messages (assuming `RegisterHandler` is never called).
//
// Deprecated: SingleHandlerRouter has been removed because it did not meet the requirements set out
// in the `Router` interface documentation. This function is only included to maintain compatibility,
// but there are limits (this version does not ignore calls to `RegisterHandler`).
func NewSingleHandlerRouter(h MessageHandler) *StandardRouter {
	return NewStandardRouterWithDefault(h)
}
