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

package com.netflix.titus.ext.cassandra.store;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titus.ext.cassandra")
public interface CassandraStoreConfiguration {

    @DefaultValue("true")
    boolean isFailOnInconsistentAgentData();

    @DefaultValue("false")
    boolean isFailOnInconsistentLoadBalancerData();

    /**
     * @return whether or not reading back records from the scheduler store should cause the system to error out.
     */
    @DefaultValue("false")
    boolean isFailOnInconsistentSchedulerData();

    /**
     * During bootstrap we run parallel queries on Cassandra cluster. If not constrained, the parallelism level
     * would be number_of_buckets * number_of_jobs_per_bucket * number_of_tasks. The concurrency limit is applied
     * at each level, so for example with limit 10 we will get up to 1000 concurrent Cassandra query requests.
     */
    @DefaultValue("10")
    int getConcurrencyLimit();

    /**
     * @return whether or not each query should have tracing enabled.
     */
    @DefaultValue("false")
    boolean isTracingEnabled();
}
