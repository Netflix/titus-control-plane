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

package com.netflix.titus.master.jobmanager.service.integration;

import org.junit.Test;

/**
 * Collection of tests focusing on Mesos integration.
 * <p>
 * TODO test when Mesos sends inconsistent data with internal state
 */
public class BatchJobMesosIntegrationTest {


    @Test
    public void testStartInitiatedFailure() throws Exception {
    }

    /**
     * Check that we do a proper cleanup if a task gets lost on Mesos.
     */
    @Test
    public void testTaskLost() throws Exception {
    }

    /**
     * Mesos driver may become temporarily unavailable. If that happens we should retry the operation later.
     */
    @Test
    public void testKillTaskErrorAndRetry() throws Exception {

    }
}
