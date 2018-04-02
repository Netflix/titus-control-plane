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

package com.netflix.titus.master.endpoint.v2;

import java.util.HashSet;
import java.util.Set;

import com.netflix.titus.api.endpoint.v2.rest.representation.TitusJobInfo;
import com.netflix.titus.api.endpoint.v2.rest.representation.TitusJobType;
import com.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskInfo;
import com.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskState;
import com.netflix.titus.master.endpoint.TitusDataGenerator;
import com.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;
import com.netflix.titus.runtime.endpoint.JobQueryCriteria;
import com.netflix.titus.testkit.model.runtime.RuntimeModelGenerator;

import static com.netflix.titus.api.model.v2.parameter.Parameters.JobType;

public final class V2TitusDataGenerator implements TitusDataGenerator<String, TitusJobSpec, TitusJobType, TitusJobInfo, TitusTaskInfo, TitusTaskState> {

    private final RuntimeModelGenerator runtimeModelGenerator;

    public V2TitusDataGenerator(String cellName) {
        this.runtimeModelGenerator = new RuntimeModelGenerator(cellName);
    }

    @Override
    public String createUser(String userName) {
        return userName;
    }

    @Override
    public TitusJobSpec createBatchJob(String name) {
        return TitusJobSpec.getSpec(runtimeModelGenerator.newJobMetadata(JobType.Batch, name));
    }

    @Override
    public TitusJobSpec createServiceJob(String name) {
        return TitusJobSpec.getSpec(runtimeModelGenerator.newJobMetadata(JobType.Service, name));
    }

    @Override
    public JobQueryCriteria<TitusTaskState, TitusJobType> getBatchTypeQuery() {
        return JobQueryCriteria.<TitusTaskState, TitusJobType>newBuilder().withJobType(TitusJobType.batch).build();
    }

    @Override
    public JobQueryCriteria<TitusTaskState, TitusJobType> getServiceTypeQuery() {
        return JobQueryCriteria.<TitusTaskState, TitusJobType>newBuilder().withJobType(TitusJobType.service).build();
    }

    @Override
    public JobQueryCriteria<TitusTaskState, TitusJobType> getAllActiveQuery() {
        return JobQueryCriteria.<TitusTaskState, TitusJobType>newBuilder().withIncludeArchived(false).build();
    }

    @Override
    public JobQueryCriteria<TitusTaskState, TitusJobType> getAllActiveAndArchivedQuery() {
        return JobQueryCriteria.<TitusTaskState, TitusJobType>newBuilder().withIncludeArchived(true).build();
    }

    @Override
    public JobQueryCriteria<TitusTaskState, TitusJobType> getAllActiveWithLimitQuery(int limit) {
        return JobQueryCriteria.<TitusTaskState, TitusJobType>newBuilder().withLimit(limit).build();
    }

    @Override
    public Set<String> getTaskIds(TitusJobInfo titusJobInfo) {
        Set<String> result = new HashSet<>();
        titusJobInfo.getTasks().forEach(t -> result.add(t.getId()));
        return result;
    }

    public RuntimeModelGenerator runtime() {
        return runtimeModelGenerator;
    }
}
