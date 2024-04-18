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

func TestConstantBackoffStrategyNoDelay(t *testing.T) {
	expected := 0 * time.Second

	noDelayStrategy := NewConstantBackoffStrategy(expected)

	noDelay := noDelayStrategy.Backoff()
	for i := 0; i < 100; i++ {
		actual := noDelay.Next()
		if actual != expected {
			t.Fatalf("expected value: `%s`, actual `%s`", expected, actual)
		}
	}
}

func TestConstantBackoffStrategyRandomValue(t *testing.T) {
	for j := 0; j < 10; j++ {
		nonZero := rand.Intn(100) + 1
		expected := time.Duration(nonZero) * time.Second

		nonZeroDelayStrategy := NewConstantBackoffStrategy(expected)

		nonZeroDelay := nonZeroDelayStrategy.Backoff()
		for i := 0; i < 100; i++ {
			actual := nonZeroDelay.Next()
			if actual != expected {
				t.Fatalf("expected value: `%s`, actual `%s`", expected, actual)
			}
		}
	}
}
