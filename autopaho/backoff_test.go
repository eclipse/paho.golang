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

// build +unittest

package autopaho

import (
	"math/rand"
	"testing"
	"time"
)

func TestConstantBackoffNoDelay(t *testing.T) {
	expected := 0 * time.Second

	noDelay := NewConstantBackoff(expected)

	for i := 0; i < 100; i++ {
		actual := noDelay(i)
		if i == 0 {
			if actual != 0 {
				t.Fatalf("First attempt should not have any delay")
			} else {
				continue
			}
		}
		if actual != expected {
			t.Fatalf("expected value: `%s`, actual `%s`", expected, actual)
		}
	}
}

func TestConstantBackoffRandomValue(t *testing.T) {
	for j := 0; j < 10; j++ {
		nonZero := rand.Intn(100) + 1
		expected := time.Duration(nonZero) * time.Second

		nonZeroDelay := NewConstantBackoff(expected)

		for i := 0; i < 100; i++ {
			actual := nonZeroDelay(i)
			if i == 0 {
				if actual != 0 {
					t.Fatalf("First attempt should not have any delay")
				} else {
					continue
				}
			}
			if actual != expected {
				t.Fatalf("expected value: `%s`, actual `%s`", expected, actual)
			}
		}
	}
}

// tests for the exponential backoff strategy implementation

func TestRandomExponentialBackoff(t *testing.T) {
	for i := 0; i < 20; i++ {
		doSetupAndTestRandomExponentialBackoff(t)
	}
}

func doSetupAndTestRandomExponentialBackoff(t *testing.T) {
	minDelayInMillisLowerBound := int64(500)                           // 500ms
	minDelayInMillisUpperBound := minDelayInMillisLowerBound + 5*1_000 // +5s
	minDelayInMillis := randRange(
		minDelayInMillisLowerBound,
		minDelayInMillisUpperBound,
	)

	minDelay := time.Duration(minDelayInMillis) * time.Millisecond

	// set up a partially random initial max backoff time
	initialMaxDelayInMillisLowerBound := minDelayInMillis + 500                       // +500ms
	initialMaxDelayInMillisUpperBound := initialMaxDelayInMillisLowerBound + 30*1_000 // +30s
	initialMaxDelayInMillis := randRange(
		initialMaxDelayInMillisLowerBound,
		initialMaxDelayInMillisUpperBound,
	)

	initialMaxDelay := time.Duration(initialMaxDelayInMillis) * time.Millisecond

	// set up a partially random max backoff time
	maxDelayMillisLowerBound := minDelayInMillis + 30*60*1_000           // +30min
	maxDelayInMillisUpperBound := maxDelayMillisLowerBound + 60*60*1_000 // +60min
	maxDelayInMillis := randRange(
		maxDelayMillisLowerBound,
		maxDelayInMillisUpperBound,
	)

	maxDelay := time.Duration(maxDelayInMillis) * time.Millisecond

	// set up factor for the next variation
	const factor = 1.6

	exponentialBackoff := NewExponentialBackoff(
		minDelay,
		maxDelay,
		initialMaxDelay,
		factor,
	)

	// create many backoffs and test they are within constraints
	for i := 0; i < 50; i++ {
		actual := exponentialBackoff(i)
		if i == 0 {
			if actual != 0 {
				t.Fatalf("First attempt should not have any delay")
			} else {
				continue
			}
		}
		if i == 0 && initialMaxDelay < actual {
			t.Fatalf("Actual backoff value: `%s` was higher than configured initial maximum: `%s`", actual, initialMaxDelay)
		}
		if actual < minDelay {
			t.Fatalf("Actual backoff value: `%s` was less than configured minimum: `%s`", actual, minDelay)
		}
		if maxDelay < actual {
			t.Fatalf("Actual backoff value: `%s` was higher than configured maximum: `%s`", actual, maxDelay)
		}
	}
}
