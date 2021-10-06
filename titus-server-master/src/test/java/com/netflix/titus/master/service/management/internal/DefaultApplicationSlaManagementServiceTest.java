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

package com.netflix.titus.master.service.management.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.api.store.v2.ApplicationSlaStore;
import com.netflix.titus.testkit.data.core.ApplicationSlaGenerator;
import com.netflix.titus.testkit.data.core.ApplicationSlaSample;
import com.netflix.titus.testkit.rx.ObservableRecorder;
import org.junit.Test;
import rx.Observable;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static com.netflix.titus.master.service.management.ApplicationSlaManagementService.DEFAULT_APPLICATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultApplicationSlaManagementServiceTest {

    private final TestScheduler testScheduler = Schedulers.test();

    private final ApplicationSlaStore storage = mock(ApplicationSlaStore.class);

    private final ApplicationSlaGenerator generator = new ApplicationSlaGenerator(ApplicationSlaSample.CriticalSmall);

    private final DefaultApplicationSlaManagementService slaManagementService = new DefaultApplicationSlaManagementService(
            storage,
            null
    );

    @Test
    public void testGetAllApplicationSLAs() throws Exception {
        when(storage.findAll()).thenReturn(Observable.from(generator.next(2)));

        List<ApplicationSLA> result = new ArrayList<>(slaManagementService.getApplicationSLAs());
        assertThat(result).hasSize(2);
    }

    @Test
    public void testGetApplicationByName() throws Exception {
        ApplicationSLA myApp = ApplicationSlaSample.CriticalSmall.build();
        when(storage.findByName(myApp.getAppName())).thenReturn(Observable.just(myApp));

        ApplicationSLA result = slaManagementService.getApplicationSLA(myApp.getAppName());
        assertThat(result.getAppName()).isEqualTo(myApp.getAppName());
        assertThat(result.getSchedulerName()).isEqualTo(myApp.getSchedulerName());
    }

    @Test
    public void testFindApplicationByName() {
        ApplicationSLA myApp = ApplicationSlaSample.CriticalSmall.build();
        when(storage.findByName(myApp.getAppName())).thenReturn(Observable.just(myApp));

        Optional<ApplicationSLA> existing = slaManagementService.findApplicationSLA(myApp.getAppName());
        assertThat(existing).hasValueSatisfying(v -> assertThat(v.getAppName()).isEqualTo(myApp.getAppName()));

        Optional<ApplicationSLA> absent = slaManagementService.findApplicationSLA("absent");
        assertThat(absent).isEmpty();
    }

    @Test
    public void testGetApplicationBySchedulerName() throws Exception {
        ApplicationSLA myApp = ApplicationSlaSample.CriticalSmallKubeScheduler.build();
        when(storage.findBySchedulerName(myApp.getSchedulerName())).thenReturn(Observable.just(myApp));

        List<ApplicationSLA> result = new ArrayList<>(slaManagementService.getApplicationSLAsForScheduler(myApp.getSchedulerName()));
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getAppName()).isEqualTo(myApp.getAppName());
        assertThat(result.get(0).getSchedulerName()).isEqualTo(myApp.getSchedulerName());
    }

    @Test
    public void testGetApplicationBySchedulerNameFailure() {
        ApplicationSLA myApp = ApplicationSlaSample.CriticalSmallKubeScheduler.build();
        Observable observable = Observable.error(new RuntimeException("generic failure"));
        when(storage.findBySchedulerName(myApp.getSchedulerName())).thenReturn(observable);

        assertThatThrownBy(() -> slaManagementService.getApplicationSLAsForScheduler(myApp.getSchedulerName())).isInstanceOf(RuntimeException.class).hasMessage("generic failure");
    }

    @Test
    public void testAddPersistsApplicationSlaAndUpdatesCapacityRequirements() throws Exception {
        ApplicationSLA myApp = ApplicationSlaSample.CriticalSmall.build();
        when(storage.findAll()).thenReturn(Observable.just(myApp));
        when(storage.create(myApp)).thenReturn(Observable.empty());

        ObservableRecorder<Void> cpmRecorder = ObservableRecorder.newRecorder(Observable.empty());

        // First add new application SLA, which will queue capacity change update
        slaManagementService.addApplicationSLA(myApp).toBlocking().firstOrDefault(null);

        // Check that capacityAllocationService is triggered correctly
        testScheduler.triggerActions();
        assertThat(cpmRecorder.numberOfFinishedSubscriptions()).isEqualTo(1);
    }

    @Test
    public void testRemovePersistsApplicationSlaAndUpdatesCapacityRequirements() throws Exception {
        ApplicationSLA myApp = ApplicationSlaSample.CriticalSmall.build();
        when(storage.findAll()).thenReturn(Observable.just(myApp));
        when(storage.remove(myApp.getAppName())).thenReturn(Observable.empty());

        ObservableRecorder<Void> cpmRecorder = ObservableRecorder.newRecorder(Observable.empty());

        // First add new application SLA, which will queue capacity change update
        slaManagementService.removeApplicationSLA(myApp.getAppName()).toBlocking().firstOrDefault(null);

        // Check that capacityAllocationService is triggered correctly
        testScheduler.triggerActions();
        assertThat(cpmRecorder.numberOfFinishedSubscriptions()).isEqualTo(1);
    }

    @Test
    public void testRemoveNotAllowedForDefaultApplication() throws Exception {
        try {
            slaManagementService.removeApplicationSLA(DEFAULT_APPLICATION).toBlocking().firstOrDefault(null);
            fail(DEFAULT_APPLICATION + " cannot be removed");
        } catch (IllegalArgumentException e) {
        }
    }
}