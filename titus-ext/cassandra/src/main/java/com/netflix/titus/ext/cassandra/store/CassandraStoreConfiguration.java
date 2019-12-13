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

import java.util.Collection;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titus.ext.cassandra")
public interface CassandraStoreConfiguration {

    @DefaultValue("dev")
    String getV2KeySpace();

    @DefaultValue("true")
    boolean isFailOnInconsistentCapacityGroupData();

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
     * Concurrency limit for load balancer target write operations (bulk INSERTs), in number of concurrent queries that
     * can be running per method invocation in {@link CassandraLoadBalancerStore#addOrUpdateTargets(Collection)}.
     *
     * @see CassandraLoadBalancerStore
     */
    @DefaultValue("10")
    int getLoadBalancerWriteConcurrencyLimit();

    /**
     * Concurrency limit for load balancer target DELETEs (bulk CQL lightweight transactions), in number of concurrent
     * queries that can be running per {@link CassandraLoadBalancerStore#removeDeregisteredTargets(Collection)}
     * invocation.
     * <p>
     * Empirical data has shown that there are no benefits in running concurrent LWT DELETEs on the same partition key,
     * so the default is <tt>1</tt> (no concurrency: all LWT DELETEs are serialized). In fact, concurrent LWTs on the
     * same partition key (<tt>load_balancer_id</tt> in this case) has shown to put pressure on C* coordinators and to
     * be overall slower than serializing all calls.
     *
     * @see CassandraLoadBalancerStore
     */
    @DefaultValue("1")
    int getLoadBalancerDeleteConcurrencyLimit();

    /**
     * @return whether or not each query should have tracing enabled.
     */
    @DefaultValue("false")
    boolean isTracingEnabled();

}
