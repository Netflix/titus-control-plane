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

package com.netflix.titus.master.endpoint;

import java.util.Set;

import com.netflix.titus.runtime.endpoint.JobQueryCriteria;

/**
 * Data generator used by {@link TitusServiceGatewayTestCompatibilityTestSuite}.
 */
public interface TitusDataGenerator<USER, JOB_SPEC, JOB_TYPE extends Enum<JOB_TYPE>, JOB, TASK, TASK_STATE> {
    JOB_SPEC createBatchJob(String name);

    JOB_SPEC createServiceJob(String name);

    USER createUser(String userName);

    JobQueryCriteria<TASK_STATE, JOB_TYPE> getBatchTypeQuery();

    JobQueryCriteria<TASK_STATE, JOB_TYPE> getServiceTypeQuery();

    JobQueryCriteria<TASK_STATE, JOB_TYPE> getAllActiveQuery();

    JobQueryCriteria<TASK_STATE, JOB_TYPE> getAllActiveAndArchivedQuery();

    JobQueryCriteria<TASK_STATE, JOB_TYPE> getAllActiveWithLimitQuery(int limit);

    Set<String> getTaskIds(JOB job);
}
