/*
 * Copyright 2021 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.titus.common.util.retry.internal;

import java.util.concurrent.TimeUnit;

import com.netflix.titus.common.util.retry.Retryer;
import com.netflix.titus.common.util.retry.Retryers;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MaxManyRetryersTest {

    @Test
    public void testMany() {
        // 10 / 1 (the expected delay of the first and the second retryer at this stage)
        Retryer first = Retryers.max(
                Retryers.interval(10, TimeUnit.MILLISECONDS, 4),
                Retryers.exponentialBackoff(1, 100, TimeUnit.MILLISECONDS)
        );
        assertThat(first.getDelayMs()).contains(10L);

        // 10 / 2
        Retryer second = first.retry();
        assertThat(second.getDelayMs()).contains(10L);

        // 10 / 4
        Retryer third = second.retry();
        assertThat(third.getDelayMs()).contains(10L);

        // 10 / 8
        Retryer fourth = third.retry();
        assertThat(fourth.getDelayMs()).contains(10L);

        // 10 / 16
        Retryer fifth = fourth.retry();
        assertThat(fifth.getDelayMs()).contains(16L);

        // retry limit reached
        Retryer sixth = fifth.retry();
        assertThat(sixth.getDelayMs()).isEmpty();
    }

    @Test
    public void testClosed() {
        Retryer retryer = Retryers.max(
                Retryers.interval(10, TimeUnit.MILLISECONDS, 4),
                Retryers.never()
        );
        assertThat(retryer.getDelayMs()).isEmpty();
    }
}