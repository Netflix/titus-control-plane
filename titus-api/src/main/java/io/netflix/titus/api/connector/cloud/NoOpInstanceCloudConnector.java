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

package io.netflix.titus.api.connector.cloud;

import java.util.List;
import java.util.Optional;
import javax.inject.Singleton;

import io.netflix.titus.api.model.ResourceDimension;
import io.netflix.titus.common.util.tuple.Either;
import rx.Completable;
import rx.Observable;

@Singleton
public class NoOpInstanceCloudConnector implements InstanceCloudConnector {
    @Override
    public Observable<List<InstanceGroup>> getInstanceGroups() {
        return Observable.empty();
    }

    @Override
    public Observable<List<InstanceGroup>> getInstanceGroups(List<String> instanceGroupIds) {
        return Observable.empty();
    }

    @Override
    public Observable<List<InstanceLaunchConfiguration>> getInstanceLaunchConfiguration(List<String> launchConfigurationIds) {
        return Observable.empty();
    }

    @Override
    public ResourceDimension getInstanceTypeResourceDimension(String instanceType) {
        throw CloudConnectorException.unrecognizedInstanceType(instanceType);
    }

    @Override
    public Observable<List<Instance>> getInstances(List<String> instanceIds) {
        return Observable.empty();
    }

    @Override
    public Completable updateCapacity(String instanceGroupId, Optional<Integer> min, Optional<Integer> desired) {
        return Completable.complete();
    }

    @Override
    public Observable<List<Either<Boolean, Throwable>>> terminateInstances(String instanceGroup, List<String> instanceIds, boolean shrink) {
        return Observable.empty();
    }
}
