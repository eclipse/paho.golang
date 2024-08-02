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
	"math/rand"
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

////////////////////////////////////////////////////////////////////////////////
// implementation for an exponential backoff strategy
////////////////////////////////////////////////////////////////////////////////

type ExponentialBackoffStrategy struct {
	minDelayMillis        int64   // lower bound for computed backoff
	maxDelayMillis        int64   // maximum upper bound for computed backoff
	initialMaxDelayMillis int64   // initial max value which wiil incerease exponentially up to the max delay
	factor                float32 // factor for the exponential increase of initial max delay
}

// The ExponentialBackoffStrategy provides a backoff duration within a range
// that increases exponentially up to a specified max value.
//
// The backoff duration is computed as a random value between the fixed min and the current max value.
// The current max is updated by multiplying the current max with the factor up the the max value.
//
// Implementation note:
//
//	For simplicity the backoff uses numbers instead of duration.
func (ebs *ExponentialBackoffStrategy) Backoff() Backoff {
	// only "moving part",
	// will be multiplied by "factor" up to the max value on each Next() call
	movingMaxMillis := ebs.initialMaxDelayMillis

	// Computes the next backoff duration
	// which will be a random value between the min and the current max value.
	computeDuration := func() time.Duration {
		randomMillisInRange := randRange(ebs.minDelayMillis, movingMaxMillis)

		return time.Duration(randomMillisInRange) * time.Millisecond
	}

	// Updates the current max value by multiplying it with the factor
	// and ensures it does not exceed the configured max value.
	updateRange := func() {
		// do nothing when max value is already reached
		if movingMaxMillis == ebs.maxDelayMillis {
			return
		}

		nextMaxMillis := int64(float32(movingMaxMillis) * ebs.factor)

		// ensure we stay in range
		// check for overflow of range OR numerical overflow
		if ebs.maxDelayMillis < nextMaxMillis || nextMaxMillis < ebs.minDelayMillis {
			nextMaxMillis = ebs.maxDelayMillis
		}

		movingMaxMillis = nextMaxMillis
	}

	return &BackoffDelegate{
		next: func() time.Duration {
			defer updateRange()

			return computeDuration()
		},
	}
}

func NewExponentialBackoffStrategy(
	minDelay time.Duration,
	maxDelay time.Duration,
	initialMaxDelay time.Duration,
	factor float32,
) *ExponentialBackoffStrategy {
	if minDelay <= 0 {
		panic("min delay must NOT be less than or equal to: 0")
	}
	if maxDelay <= minDelay {
		panic("max delay must NOT be less than or equal to: min delay")
	}
	if initialMaxDelay < minDelay || maxDelay < initialMaxDelay {
		panic("initial max delay must be in range of: (min, max) delay")
	}
	if factor <= 1 {
		panic("factor must NOT be less than or equal to: 1")
	}

	return &ExponentialBackoffStrategy{
		minDelayMillis:        minDelay.Milliseconds(),
		maxDelayMillis:        maxDelay.Milliseconds(),
		initialMaxDelayMillis: initialMaxDelay.Milliseconds(),
		factor:                factor,
	}
}

// DefaultExponentialBackoffStrategy returns a new ExponentialBackoffStrategy with default values.
//
// The default values are:
//   - min delay:          5 seconds
//   - max delay:         10 minutes
//   - initial max delay: 10 seconds
//   - factor:             1.5
func DefaultExponentialBackoffStrategy() *ExponentialBackoffStrategy {
	return NewExponentialBackoffStrategy(
		05*time.Second, // minDelay
		10*time.Minute, // maxDelay
		10*time.Second, // initialMaxDelay
		1.5,            // factor
	)
}

////////////////////////////////////////////////////////////////////////////////
// util functions
////////////////////////////////////////////////////////////////////////////////

// Returns a random number in the range of [start, end] (inclusive)
func randRange(start int64, end int64) int64 {
	normalizedRange := end - start + 1

	return rand.Int63n(normalizedRange) + start
}
