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

package com.netflix.titus.master.service.management;

import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.master.service.management.CapacityManagementConfiguration.ResourceDimensionConfiguration;
import com.netflix.titus.master.store.ApplicationSlaStore;
import com.netflix.titus.master.store.exception.NotFoundException;
import com.netflix.titus.testkit.data.core.ApplicationSlaSample;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;

import static com.netflix.titus.master.service.management.ApplicationSlaManagementService.DEFAULT_APPLICATION;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ManagementSubsystemInitializerTest {

    private final ResourceDimensionConfiguration resourceConfiguration = mock(ResourceDimensionConfiguration.class);

    private final CapacityManagementConfiguration configuration = mock(CapacityManagementConfiguration.class);

    private final ApplicationSlaStore applicationStore = mock(ApplicationSlaStore.class);

    @Before
    public void setUp() throws Exception {
        when(resourceConfiguration.getCPU()).thenReturn(4.0);
        when(resourceConfiguration.getMemoryMB()).thenReturn(1024);
        when(resourceConfiguration.getDiskMB()).thenReturn(100);
        when(resourceConfiguration.getNetworkMbs()).thenReturn(100);

        when(configuration.getDefaultApplicationInstanceCount()).thenReturn(10);
        when(configuration.getDefaultApplicationResourceDimension()).thenReturn(resourceConfiguration);
    }

    @Test
    public void testDefaultApplicationSlaIsCreatedIfNotDefined() throws Exception {
        when(applicationStore.findByName(DEFAULT_APPLICATION)).thenReturn(
                Observable.error(new NotFoundException(ApplicationSLA.class, "missing"))
        );
        when(applicationStore.create(any())).thenReturn(Observable.empty());

        new ManagementSubsystemInitializer(configuration, applicationStore).enterActiveMode();

        verify(applicationStore, times(1)).findByName(DEFAULT_APPLICATION);
        verify(applicationStore, times(1)).create(any());
    }

    @Test
    public void testDefaultApplicationSlaIsNotOverriddenIfPresent() throws Exception {
        when(applicationStore.findByName(DEFAULT_APPLICATION)).thenReturn(Observable.just(
                ApplicationSlaSample.CriticalLarge.builder().withAppName(DEFAULT_APPLICATION).build()
        ));

        new ManagementSubsystemInitializer(configuration, applicationStore).enterActiveMode();

        verify(applicationStore, times(1)).findByName(DEFAULT_APPLICATION);
        verify(applicationStore, times(0)).create(any());
    }
}