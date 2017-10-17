/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.endpoint;

/**
 * Endpoint models asserts used by {@link TitusServiceGatewayTestCompatibilityTestSuite}.
 */
public interface EndpointModelAsserts<USER, JOB_SPEC, JOB_TYPE extends Enum<JOB_TYPE>, JOB, TASK, TASK_STATE> {

    void assertJobId(JOB actualJob, String jobId);

    void assertSpecOfJob(JOB job, JOB_SPEC jobSpec);

    void assertJobKilled(JOB job);

    void assertServiceJobSize(JOB job, int desired, int min, int max);

    void assertJobInService(JOB job, boolean inService);
}
