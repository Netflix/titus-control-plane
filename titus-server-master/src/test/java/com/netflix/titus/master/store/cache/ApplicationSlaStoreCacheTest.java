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

package com.netflix.titus.master.store.cache;

import java.util.ArrayList;
import java.util.List;

import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.master.store.ApplicationSlaStore;
import com.netflix.titus.master.store.exception.NotFoundException;
import com.netflix.titus.testkit.data.core.ApplicationSlaGenerator;
import com.netflix.titus.testkit.data.core.ApplicationSlaSample;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ApplicationSlaStoreCacheTest {

    private static final int INIT_SIZE = 2;

    private final ApplicationSlaGenerator generator = new ApplicationSlaGenerator(ApplicationSlaSample.CriticalSmall);

    private final List<ApplicationSLA> initSet = new ArrayList<>(generator.next(INIT_SIZE));

    private final ApplicationSlaStore delegate = mock(ApplicationSlaStore.class);

    private ApplicationSlaStoreCache store;

    @Before
    public void setUp() throws Exception {
        when(delegate.findAll()).thenReturn(Observable.from(initSet));
        store = new ApplicationSlaStoreCache(delegate);
        store.enterActiveMode();
    }

    @Test
    public void testFindAll() throws Exception {
        List<ApplicationSLA> all = store.findAll().toList().toBlocking().first();
        assertThat(all).hasSize(INIT_SIZE);

        verify(delegate, times(1)).findAll(); // 1 from the construction time
    }

    @Test
    public void testFindByName() throws Exception {
        ApplicationSLA applicationSLA = initSet.get(0);
        ApplicationSLA result = store.findByName(applicationSLA.getAppName()).toBlocking().first();
        assertThat(result).isEqualTo(applicationSLA);
    }

    @Test
    public void testRemove() throws Exception {
        when(delegate.remove(anyString())).thenReturn(Observable.empty());

        ApplicationSLA applicationSLA = initSet.get(0);
        store.remove(applicationSLA.getAppName()).toBlocking().firstOrDefault(null);

        try {
            store.findByName(applicationSLA.getAppName()).toBlocking().first();
            fail("Expected to fail as the entity has been removed");
        } catch (NotFoundException ignore) {
        }
    }
}