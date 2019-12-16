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

package com.netflix.titus.ext.cassandra.store;

public class TestCassandraStoreConfiguration implements CassandraStoreConfiguration {
    @Override
    public String getV2KeySpace() {
        return "dev";
    }

    @Override
    public boolean isFailOnInconsistentCapacityGroupData() {
        return true;
    }

    @Override
    public boolean isFailOnInconsistentAgentData() {
        return true;
    }

    @Override
    public boolean isFailOnInconsistentLoadBalancerData() {
        return true;
    }

    @Override
    public boolean isFailOnInconsistentSchedulerData() {
        return true;
    }

    @Override
    public int getConcurrencyLimit() {
        return 10;
    }

    @Override
    public int getLoadBalancerWriteConcurrencyLimit() {
        return 100;
    }

    @Override
    public int getLoadBalancerDeleteConcurrencyLimit() {
        return 10;
    }

    @Override
    public boolean isTracingEnabled() {
        return false;
    }
}
