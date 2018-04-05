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

package com.netflix.titus.common.jhiccup;

import com.netflix.spectator.api.DefaultRegistry;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HiccupMeterTest {

    private final HiccupRecorderConfiguration config = mock(HiccupRecorderConfiguration.class);

    @Before
    public void setUp() throws Exception {
        when(config.getTaskExecutionDeadlineMs()).thenReturn(1000L);
        when(config.getReportingIntervalMs()).thenReturn(1000L);
    }

    @Test
    public void testStartupShutdownSequence() throws Exception {
        HiccupMeter hiccupMeter = new HiccupMeter(config, new DefaultRegistry());
        hiccupMeter.shutdown();
    }
}