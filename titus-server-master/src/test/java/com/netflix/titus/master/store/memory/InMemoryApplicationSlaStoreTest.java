/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.master.store.memory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.api.store.v2.ApplicationSlaStore;
import com.netflix.titus.api.store.v2.ApplicationSlaStoreCache;
import org.junit.Test;

import static com.netflix.titus.testkit.data.core.ApplicationSlaSample.CriticalSmall;
import static com.netflix.titus.testkit.data.core.ApplicationSlaSample.DefaultFlex;
import static org.assertj.core.api.Assertions.assertThat;

public class InMemoryApplicationSlaStoreTest {

    private static final long TIMEOUT = 30_000L;

    @Test
    public void testStoreAndRetrieveCapacityGroups() {
        ApplicationSlaStore store = createStore();
        ApplicationSLA capacityGroup1 = DefaultFlex.build();
        ApplicationSLA capacityGroup2 = CriticalSmall.builder()
                .withSchedulerName("kubeScheduler").build();

        List<ApplicationSLA> capacityGroups = Arrays.asList(capacityGroup1, capacityGroup2);

        // Create initial
        capacityGroups.forEach(
                capacityGroup -> assertThat(store
                        .create(capacityGroup)
                        .toCompletable()
                        .await(TIMEOUT, TimeUnit.MILLISECONDS)).isTrue());
        List<ApplicationSLA> result;

        // find all
        result = store.findAll().toList().timeout(TIMEOUT, TimeUnit.MILLISECONDS).toBlocking().first();
        assertThat(result).containsExactlyInAnyOrder(capacityGroup1, capacityGroup2);

        // find by name
        result = store.findByName(capacityGroup1.getAppName()).toList().timeout(TIMEOUT, TimeUnit.MILLISECONDS).toBlocking().first();
        assertThat(result).containsExactlyInAnyOrder(capacityGroup1);

        // find by scheduler name
        result = store.findBySchedulerName("fenzo").toList().timeout(TIMEOUT, TimeUnit.MILLISECONDS).toBlocking().first();
        assertThat(result).containsExactlyInAnyOrder(capacityGroup1);

        result = store.findBySchedulerName("kubeScheduler").toList().timeout(TIMEOUT, TimeUnit.MILLISECONDS).toBlocking().first();
        assertThat(result).containsExactlyInAnyOrder(capacityGroup2);

        // find by non-existent scheduler name
        result = store.findBySchedulerName("").toList().timeout(TIMEOUT, TimeUnit.MILLISECONDS).toBlocking().first();
        assertThat(result).isEmpty();

        // update scheduler name and verify
        ApplicationSLA updatedCapacityGroup = ApplicationSLA.newBuilder(capacityGroup1).withSchedulerName("kubeScheduler").build();
        assertThat(store.create(updatedCapacityGroup).toCompletable().await(TIMEOUT, TimeUnit.MILLISECONDS)).isTrue();
        result = store.findAll().toList().timeout(TIMEOUT, TimeUnit.MILLISECONDS).toBlocking().first();
        assertThat(result).containsExactlyInAnyOrder(updatedCapacityGroup, capacityGroup2);

        // remove
        assertThat(store.remove(updatedCapacityGroup.getAppName()).toCompletable().await(TIMEOUT, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(store.remove(capacityGroup2.getAppName()).toCompletable().await(TIMEOUT, TimeUnit.MILLISECONDS)).isTrue();

        assertThat(store.findAll().toList().timeout(TIMEOUT, TimeUnit.MILLISECONDS).toBlocking().first()).isEmpty();
    }

    private ApplicationSlaStore createStore() {
        ApplicationSlaStoreCache applicationSlaStoreCache = new ApplicationSlaStoreCache(new InMemoryApplicationSlaStore());
        applicationSlaStoreCache.enterActiveMode();
        return applicationSlaStoreCache;
    }
}