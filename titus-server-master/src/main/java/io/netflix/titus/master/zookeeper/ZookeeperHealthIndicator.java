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

package io.netflix.titus.master.zookeeper;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.runtime.health.api.Health;
import com.netflix.runtime.health.api.HealthIndicator;
import com.netflix.runtime.health.api.HealthIndicatorCallback;
import org.apache.curator.framework.imps.CuratorFrameworkState;

@Singleton
class ZookeeperHealthIndicator implements HealthIndicator {

    private final CuratorService curatorService;

    @Inject
    public ZookeeperHealthIndicator(CuratorService curatorService) {
        this.curatorService = curatorService;
    }

    @Override
    public void check(HealthIndicatorCallback healthCallback) {
        CuratorFrameworkState state = curatorService.getCurator().getState();
        if (state == CuratorFrameworkState.STARTED) {
            healthCallback.inform(Health.healthy().build());
        } else {
            healthCallback.inform(Health.unhealthy().withDetail("state", state).build());
        }
    }
}
