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

package autopaho

import (
	"time"
)

// The BackoffStrategy is a container for the configuration of a given strategy
// whereas the returned Backoff is the actual implementation of the strategy.
//
// As such BackoffStrategy instances are supposed to be thread safe and freely sharable between users.
type BackoffStrategy interface {
	// Returns a configured backoff instance.
	Backoff() Backoff
}

// Backoff represents a configured instance of the strategy which computes the
// next backoff duration.
//
// NOTE: Generally instances are NOT supposed to be reused or shared.
type Backoff interface {
	// Returns the next backoff duration.
	Next() time.Duration
}

////////////////////////////////////////////////////////////////////////////////
// helper structs for backoff implementations
////////////////////////////////////////////////////////////////////////////////

// The BackoffDelegate implements the Backoff interface and delegates backoff
// computation to the internal function property.
type BackoffDelegate struct {
	next func() time.Duration
}

func (b *BackoffDelegate) Next() time.Duration {
	return b.next()
}

////////////////////////////////////////////////////////////////////////////////
// implementation for constant backoff
////////////////////////////////////////////////////////////////////////////////

// The ConstantBackoffStrategy implements the BackoffStrategy interface and
// provides instances of constant duration.
//
// As the duration is constant, the strategy and the actual backoff are thread
// safe and can be shared.
type ConstantBackoffStrategy struct {
	backoff Backoff
}

func (bs *ConstantBackoffStrategy) Backoff() Backoff {
	return bs.backoff
}

// Creates a new ConstantBackoffStrategy with the specified delay.
func NewConstantBackoffStrategy(delay time.Duration) *ConstantBackoffStrategy {
	backoff := &BackoffDelegate{
		next: func() time.Duration {
			return delay
		},
	}

	return &ConstantBackoffStrategy{
		backoff: backoff,
	}
}
