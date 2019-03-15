/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.runtime.connector.jobmanager;

import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.runtime.connector.jobmanager.replicator.JobDataReplicatorProvider;
import com.netflix.titus.runtime.jobmanager.gateway.JobServiceGateway;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JobManagementDataReplicationComponent {

    @Bean
    public JobDataReplicator getJobDataReplicator(JobServiceGateway client, TitusRuntime titusRuntime) {
        return new JobDataReplicatorProvider(client, titusRuntime).get();
    }

    @Bean
    public ReadOnlyJobOperations getReadOnlyJobOperations(JobDataReplicator replicator) {
        return new CachedReadOnlyJobOperations(replicator);
    }
}
