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

package io.netflix.titus.api.connector.cloud.noop;

import io.netflix.titus.api.appscale.model.AutoScalableTarget;
import io.netflix.titus.api.appscale.model.PolicyConfiguration;
import io.netflix.titus.api.connector.cloud.AppAutoScalingClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;

public class NoOpAppAutoScalingClient implements AppAutoScalingClient {
    private static Logger log = LoggerFactory.getLogger(NoOpAppAutoScalingClient.class);

    @Override
    public Completable createScalableTarget(String jobId, int minCapacity, int maxCapacity) {
        log.info("Created scalable target for job {}", jobId);
        return Completable.complete();
    }

    @Override
    public Observable<String> createOrUpdateScalingPolicy(String policyRefId, String jobId, PolicyConfiguration policyConfiguration) {
        log.info("Created/Updated scaling policy {} - for job {}", policyRefId, jobId);
        return Observable.just(policyRefId);
    }

    @Override
    public Completable deleteScalableTarget(String jobId) {
        log.info("Deleted scalable target for job {}", jobId);
        return Completable.complete();
    }

    @Override
    public Completable deleteScalingPolicy(String policyRefId, String jobId) {
        log.info("Deleted scaling policy {} for job {}", policyRefId, jobId);
        return Completable.complete();
    }

    @Override
    public Observable<AutoScalableTarget> getScalableTargetsForJob(String jobId) {
        AutoScalableTarget autoScalableTarget = AutoScalableTarget.newBuilder()
                .withMinCapacity(1)
                .withMaxCapacity(10)
                .build();
        log.info("Scalable targets for JobId {} - {}", jobId, autoScalableTarget);
        return Observable.just(autoScalableTarget);
    }
}
