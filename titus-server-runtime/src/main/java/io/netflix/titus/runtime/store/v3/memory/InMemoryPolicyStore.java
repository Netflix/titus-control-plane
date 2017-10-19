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

package io.netflix.titus.runtime.store.v3.memory;

import io.netflix.titus.api.appscale.model.AutoScalableTarget;
import io.netflix.titus.api.appscale.model.AutoScalingPolicy;
import io.netflix.titus.api.appscale.model.PolicyStatus;
import io.netflix.titus.api.appscale.store.AppScalePolicyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class InMemoryPolicyStore implements AppScalePolicyStore {
    private static Logger log = LoggerFactory.getLogger(InMemoryPolicyStore.class);

    private final Map<String, AutoScalingPolicy> policyMap = new ConcurrentHashMap<>();
    private final Map<String, AutoScalableTarget> scalableTargetMap = new ConcurrentHashMap<>();

    @Override
    public Completable init() {
        return Completable.complete();
    }

    @Override
    public Completable reportPolicyMetrics() {
        log.info("Reporting #policies {} ", policyMap.size());
        return Completable.complete();
    }

    @Override
    public Observable<AutoScalingPolicy> retrievePolicies() {
        return Observable.from(
                policyMap.values().stream()
                        .filter(autoScalingPolicy ->
                                autoScalingPolicy.getStatus() == PolicyStatus.Pending ||
                                        autoScalingPolicy.getStatus() == PolicyStatus.Applied ||
                                        autoScalingPolicy.getStatus() == PolicyStatus.Deleting)
                        .collect(Collectors.toList()));
    }

    @Override
    public Observable<String> storePolicy(AutoScalingPolicy autoScalingPolicy) {
        return Observable.fromCallable(() -> {
            String policyRefId = UUID.randomUUID().toString();
            AutoScalingPolicy policySaved = AutoScalingPolicy.newBuilder()
                    .withAutoScalingPolicy(autoScalingPolicy)
                    .withRefId(policyRefId)
                    .withStatus(PolicyStatus.Pending)
                    .build();
            policyMap.putIfAbsent(policyRefId, policySaved);
            return policyRefId;
        });
    }

    @Override
    public Completable updatePolicyId(String policyRefId, String policyId) {
        return Completable.fromCallable(() -> {
            AutoScalingPolicy autoScalingPolicy = policyMap.get(policyRefId);
            AutoScalingPolicy policySaved = AutoScalingPolicy.newBuilder().withAutoScalingPolicy(autoScalingPolicy).withPolicyId(policyId).build();
            return policyMap.put(policyRefId, policySaved);
        });
    }

    @Override
    public Completable updateAlarmId(String policyRefId, String alarmId) {
        return Completable.fromCallable(() -> {
            AutoScalingPolicy autoScalingPolicy = policyMap.get(policyRefId);
            AutoScalingPolicy policySaved = AutoScalingPolicy.newBuilder().withAutoScalingPolicy(autoScalingPolicy).withAlarmId(alarmId).build();
            return policyMap.put(policyRefId, policySaved);
        });
    }

    @Override
    public Completable updatePolicyStatus(String policyRefId, PolicyStatus policyStatus) {
        return Completable.fromCallable(() -> {
            AutoScalingPolicy autoScalingPolicy = policyMap.get(policyRefId);
            AutoScalingPolicy policySaved = AutoScalingPolicy.newBuilder().withAutoScalingPolicy(autoScalingPolicy).withStatus(policyStatus).build();
            return policyMap.put(policyRefId, policySaved);
        });
    }

    @Override
    public Completable updateStatusMessage(String policyRefId, String statusMessage) {
        return Completable.fromCallable(() -> {
            AutoScalingPolicy autoScalingPolicy = policyMap.get(policyRefId);
            AutoScalingPolicy policySaved = AutoScalingPolicy.newBuilder().withAutoScalingPolicy(autoScalingPolicy)
                    .withStatusMessage(statusMessage).build();
            return policyMap.put(policyRefId, policySaved);
        });
    }

    @Override
    public Observable<AutoScalingPolicy> retrievePoliciesForJob(String jobId) {
        return Observable.from(policyMap.values())
                .filter(autoScalingPolicy -> autoScalingPolicy.getJobId().equals(jobId) &&
                        (autoScalingPolicy.getStatus() == PolicyStatus.Pending ||
                                autoScalingPolicy.getStatus() == PolicyStatus.Deleting ||
                                autoScalingPolicy.getStatus() == PolicyStatus.Applied));
    }

    @Override
    public Completable updatePolicyConfiguration(AutoScalingPolicy autoScalingPolicy) {
        return Completable.fromCallable(() -> {
            AutoScalingPolicy policySaved = AutoScalingPolicy.newBuilder().withAutoScalingPolicy(autoScalingPolicy).build();
            return policyMap.put(policySaved.getRefId(), policySaved);
        });
    }


    @Override
    public Observable<AutoScalingPolicy> retrievePolicyForRefId(String policyRefId) {
        if (policyMap.containsKey(policyRefId)) {
            return Observable.just(policyMap.get(policyRefId));
        }
        return Observable.empty();
    }

    @Override
    public Completable removePolicy(String policyRefId) {
        return updatePolicyStatus(policyRefId, PolicyStatus.Deleting);
    }
}
