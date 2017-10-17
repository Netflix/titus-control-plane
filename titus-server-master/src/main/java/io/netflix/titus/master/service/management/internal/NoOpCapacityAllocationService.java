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

package io.netflix.titus.master.service.management.internal;

import java.util.List;
import java.util.stream.Collectors;

import io.netflix.titus.master.service.management.CapacityAllocationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

/**
 * Default, no-op implementation.
 */
public class NoOpCapacityAllocationService implements CapacityAllocationService {

    private static final Logger logger = LoggerFactory.getLogger(NoOpCapacityAllocationService.class);

    @Override
    public Observable<List<Integer>> limits(List<String> instanceTypes) {
        return Observable.just(instanceTypes.stream().map(itype -> 1000).collect(Collectors.toList()));
    }

    @Override
    public Observable<Void> allocate(String instanceType, int instanceCount) {
        logger.info("Capacity change request {}={}", instanceType, instanceCount);
        return Observable.empty();
    }
}
