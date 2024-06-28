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

// Backoff function to compute backoff duration for the Nth attempt
type Backoff func(attempt int) time.Duration

////////////////////////////////////////////////////////////////////////////////
// implementation for constant backoff
////////////////////////////////////////////////////////////////////////////////

// Creates a new backoff with constant delay (regardless of the attempt).
func NewConstantBackoff(delay time.Duration) Backoff {
	return func(attempt int) time.Duration {
		return delay
	}
}
