/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.titus.common.util.code;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.common.util.code.CodePointTracker.CodePoint;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SpectatorCodePointTrackerTest {

    private final Registry registry = new DefaultRegistry();

    private final SpectatorCodePointTracker tracker = new SpectatorCodePointTracker(registry);

    @Before
    public void setUp() throws Exception {
        CodePointTracker.setDefault(tracker);
    }

    @Test
    public void testCodePointHit() throws Exception {
        tracker.markReachable();
        tracker.markReachable("request2");

        assertThat(tracker.marked).hasSize(2);

        for (CodePoint first : tracker.marked.keySet()) {
            assertThat(first.getClassName()).isEqualTo(SpectatorCodePointTrackerTest.class.getName());
            assertThat(first.getMethodName()).isEqualTo("testCodePointHit");
        }
    }

    @Test
    public void testCodePointHitViaStaticMethod() throws Exception {
        CodePointTracker.mark();
        CodePointTracker.mark("request2");

        assertThat(tracker.marked).hasSize(2);

        for (CodePoint first : tracker.marked.keySet()) {
            assertThat(first.getClassName()).isEqualTo(SpectatorCodePointTrackerTest.class.getName());
            assertThat(first.getMethodName()).isEqualTo("testCodePointHitViaStaticMethod");
        }
    }
}