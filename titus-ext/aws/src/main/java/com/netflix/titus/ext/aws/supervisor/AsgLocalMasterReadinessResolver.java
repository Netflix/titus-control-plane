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

package com.netflix.titus.ext.aws.supervisor;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import com.amazonaws.services.autoscaling.AmazonAutoScalingAsync;
import com.amazonaws.services.autoscaling.model.AutoScalingGroup;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsRequest;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsResult;
import com.amazonaws.services.autoscaling.model.TagDescription;
import com.netflix.titus.api.supervisor.model.ReadinessState;
import com.netflix.titus.api.supervisor.model.ReadinessStatus;
import com.netflix.titus.api.supervisor.service.LocalMasterReadinessResolver;
import com.netflix.titus.api.supervisor.service.resolver.PollingLocalMasterReadinessResolver;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.retry.Retryers;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.ext.aws.AwsConfiguration;
import com.netflix.titus.ext.aws.AwsReactorExt;
import com.netflix.titus.ext.aws.ControlPlaneAmazonAutoScalingAsyncProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@Singleton
public class AsgLocalMasterReadinessResolver implements LocalMasterReadinessResolver {

    public static final String TAG_MASTER_ENABLED = "titus.masterEnabled";

    private static final Logger logger = LoggerFactory.getLogger(AsgLocalMasterReadinessResolver.class);

    static final ScheduleDescriptor REFRESH_SCHEDULER_DESCRIPTOR = ScheduleDescriptor.newBuilder()
            .withName(AsgLocalMasterReadinessResolver.class.getSimpleName())
            .withDescription("Local Master state resolution from ASG metadata")
            .withInitialDelay(Duration.ZERO)
            .withInterval(Duration.ofSeconds(5))
            .withRetryerSupplier(() -> Retryers.exponentialBackoff(100, 1_000, TimeUnit.MILLISECONDS, 5))
            .withTimeout(Duration.ofSeconds(5))
            .withOnErrorHandler((action, error) -> {
                logger.warn("Cannot read master ASG data: {}", error.getMessage());
                logger.debug(error.getMessage(), error);
            })
            .build();

    private final AwsConfiguration configuration;
    private final AmazonAutoScalingAsync autoScalingClient;
    private final Clock clock;

    private final PollingLocalMasterReadinessResolver poller;

    private final AtomicReference<String> lastTagValue = new AtomicReference<>();

    @Inject
    public AsgLocalMasterReadinessResolver(AwsConfiguration configuration,
                                           @Named(ControlPlaneAmazonAutoScalingAsyncProvider.NAME) AmazonAutoScalingAsync autoScalingClient,
                                           TitusRuntime titusRuntime) {
        this(configuration, autoScalingClient, REFRESH_SCHEDULER_DESCRIPTOR, titusRuntime, Schedulers.parallel());
    }

    public AsgLocalMasterReadinessResolver(AwsConfiguration configuration,
                                           AmazonAutoScalingAsync autoScalingClient,
                                           ScheduleDescriptor scheduleDescriptor,
                                           TitusRuntime titusRuntime,
                                           Scheduler scheduler) {
        this.configuration = configuration;
        this.autoScalingClient = autoScalingClient;
        this.clock = titusRuntime.getClock();

        this.poller = PollingLocalMasterReadinessResolver.newPollingResolver(
                refresh(),
                scheduleDescriptor,
                titusRuntime,
                scheduler
        );
    }

    @PreDestroy
    public void shutdown() {
        poller.shutdown();
    }

    @Override
    public Flux<ReadinessStatus> observeLocalMasterReadinessUpdates() {
        return poller.observeLocalMasterReadinessUpdates();
    }

    private Mono<ReadinessStatus> refresh() {
        return AwsReactorExt
                .<DescribeAutoScalingGroupsRequest, DescribeAutoScalingGroupsResult>toMono(
                        () -> {
                            DescribeAutoScalingGroupsRequest request = new DescribeAutoScalingGroupsRequest();
                            request.setAutoScalingGroupNames(Collections.singletonList(configuration.getTitusMasterAsgName()));
                            return request;
                        },
                        autoScalingClient::describeAutoScalingGroupsAsync
                )
                .map(this::resolveStatus);
    }

    private ReadinessStatus resolveStatus(DescribeAutoScalingGroupsResult response) {
        ReadinessState effectiveState = null;
        String message = null;

        if (response.getAutoScalingGroups().size() < 1) {
            setNewTagValue("");
            effectiveState = ReadinessState.Disabled;
            message = "ASG not found: " + configuration.getTitusMasterAsgName();
        } else {
            AutoScalingGroup autoScalingGroup = response.getAutoScalingGroups().get(0);

            if (autoScalingGroup.getTags() != null) {
                for (TagDescription tagDescription : autoScalingGroup.getTags()) {
                    if (TAG_MASTER_ENABLED.equals(tagDescription.getKey())) {
                        String value = tagDescription.getValue();
                        setNewTagValue(value);

                        boolean enabled = Boolean.valueOf(value);
                        effectiveState = enabled ? ReadinessState.Enabled : ReadinessState.Disabled;
                        message = "Set as tag on ASG: " + configuration.getTitusMasterAsgName();
                    }
                }
            }

            if (effectiveState == null) {
                setNewTagValue("");
                effectiveState = ReadinessState.Disabled;
                message = String.format("ASG tag %s not found: %s", TAG_MASTER_ENABLED, configuration.getTitusMasterAsgName());
            }
        }

        return ReadinessStatus.newBuilder()
                .withState(effectiveState)
                .withMessage(message)
                .withTimestamp(clock.wallTime())
                .build();
    }

    private void setNewTagValue(String newValue) {
        if (!newValue.equals(lastTagValue.get())) {
            logger.info("New master state tag value: previous={}, new={}", lastTagValue.get(), newValue);
        }
        lastTagValue.set(newValue);
    }
}
