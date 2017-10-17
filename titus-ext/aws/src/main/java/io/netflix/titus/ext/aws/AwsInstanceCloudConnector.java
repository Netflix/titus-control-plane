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

package io.netflix.titus.ext.aws;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.amazonaws.services.autoscaling.AmazonAutoScalingAsync;
import com.amazonaws.services.autoscaling.model.AutoScalingGroup;
import com.amazonaws.services.autoscaling.model.AutoScalingInstanceDetails;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsRequest;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingInstancesRequest;
import com.amazonaws.services.autoscaling.model.DescribeLaunchConfigurationsRequest;
import com.amazonaws.services.autoscaling.model.DetachInstancesRequest;
import com.amazonaws.services.autoscaling.model.LaunchConfiguration;
import com.amazonaws.services.autoscaling.model.SuspendedProcess;
import com.amazonaws.services.autoscaling.model.UpdateAutoScalingGroupRequest;
import com.amazonaws.services.ec2.AmazonEC2Async;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import io.netflix.titus.api.connector.cloud.CloudConnectorException;
import io.netflix.titus.api.connector.cloud.Instance;
import io.netflix.titus.api.connector.cloud.Instance.InstanceState;
import io.netflix.titus.api.connector.cloud.InstanceCloudConnector;
import io.netflix.titus.api.connector.cloud.InstanceGroup;
import io.netflix.titus.api.connector.cloud.InstanceLaunchConfiguration;
import io.netflix.titus.api.model.ResourceDimension;
import io.netflix.titus.common.aws.AwsInstanceDescriptor;
import io.netflix.titus.common.aws.AwsInstanceType;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.common.util.guice.ProxyType;
import io.netflix.titus.common.util.guice.annotation.Activator;
import io.netflix.titus.common.util.guice.annotation.ProxyConfiguration;
import io.netflix.titus.common.util.rx.ObservableExt;
import io.netflix.titus.common.util.tuple.Either;
import io.netflix.titus.common.util.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import static java.util.Collections.singletonList;

@Singleton
@ProxyConfiguration(types = {ProxyType.ActiveGuard, ProxyType.Logging, ProxyType.Spectator})
public class AwsInstanceCloudConnector implements InstanceCloudConnector {

    private static final Logger logger = LoggerFactory.getLogger(AwsInstanceCloudConnector.class);

    static final int AWS_PAGE_MAX = 100;
    static final int AWS_INSTANCE_ID_MAX = 50;
    static final int AWS_PARALLELISM = 5;

    static final int AWS_MAX_INSTANCE_DETACH = 20;
    static final long AWS_OPERATION_DELAY_MS = 500;

    static final String TAG_TERMINATE = "TitusAgentPendingTermination";
    static final String TAG_ASG_LINK = "aws:autoscaling:groupName";

    private final AwsConfiguration configuration;
    private final AmazonEC2Async ec2Client;
    private final AmazonAutoScalingAsync autoScalingClient;
    private final Scheduler scheduler;

    public AwsInstanceCloudConnector(AwsConfiguration configuration,
                                     AmazonEC2Async ec2Client,
                                     AmazonAutoScalingAsync autoScalingClient,
                                     Scheduler scheduler) {
        this.configuration = configuration;
        this.ec2Client = ec2Client;
        this.autoScalingClient = autoScalingClient;
        this.scheduler = scheduler;
    }

    @Inject
    public AwsInstanceCloudConnector(AwsConfiguration configuration,
                                     AmazonEC2Async ec2Client,
                                     AmazonAutoScalingAsync autoScalingClient) {
        this(configuration, ec2Client, autoScalingClient, Schedulers.computation());
    }

    @Activator
    public void enterActiveMode() {
        // We need this empty method, to mark this service as activated.
    }

    @PreDestroy
    public void shutdown() {
        autoScalingClient.shutdown();
    }

    @Override
    public Observable<List<InstanceGroup>> getInstanceGroups() {
        PageCollector<DescribeAutoScalingGroupsRequest, AutoScalingGroup> pageCollector = new PageCollector<>(
                token -> new DescribeAutoScalingGroupsRequest().withMaxRecords(AWS_PAGE_MAX).withNextToken(token),
                request -> toObservable(() -> autoScalingClient.describeAutoScalingGroupsAsync(request))
                        .map(result -> Pair.of(result.getAutoScalingGroups(), result.getNextToken()))
        );
        return pageCollector.getAll()
                .map(this::toInstanceGroups)
                .timeout(configuration.getInstanceGroupsFetchTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Observable<List<InstanceGroup>> getInstanceGroups(List<String> instanceGroupIds) {
        if (instanceGroupIds.isEmpty()) {
            return Observable.just(Collections.emptyList());
        }
        return toObservable(() -> {
            DescribeAutoScalingGroupsRequest request = new DescribeAutoScalingGroupsRequest().withAutoScalingGroupNames(instanceGroupIds);
            return autoScalingClient.describeAutoScalingGroupsAsync(request);
        }).map(
                response -> toInstanceGroups(response.getAutoScalingGroups())
        ).timeout(configuration.getAwsRequestTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Observable<List<InstanceLaunchConfiguration>> getInstanceLaunchConfiguration(List<String> launchConfigurationIds) {
        if (launchConfigurationIds.isEmpty()) {
            return Observable.just(Collections.emptyList());
        }
        return toObservable(() -> {
            DescribeLaunchConfigurationsRequest request = new DescribeLaunchConfigurationsRequest().withLaunchConfigurationNames(launchConfigurationIds);
            return autoScalingClient.describeLaunchConfigurationsAsync(request);
        }).map(
                response -> toVmLaunchConfigurations(response.getLaunchConfigurations())
        ).timeout(configuration.getAwsRequestTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    @Override
    public ResourceDimension getInstanceTypeResourceDimension(String instanceType) {
        AwsInstanceDescriptor descriptor;
        try {
            descriptor = AwsInstanceType.withName(instanceType).getDescriptor();
        } catch (Exception ignore) {
            throw CloudConnectorException.unrecognizedInstanceType(instanceType);
        }

        return ResourceDimension.newBuilder()
                .withCpus(descriptor.getvCPUs())
                .withGpu(descriptor.getvGPUs())
                .withMemoryMB(descriptor.getMemoryGB() * 1024)
                .withDiskMB(descriptor.getStorageGB() * 1024)
                .withNetworkMbs(descriptor.getNetworkMbs())
                .build();
    }

    @Override
    public Observable<List<Instance>> getInstances(List<String> instanceIds) {
        if (instanceIds.isEmpty()) {
            return Observable.just(Collections.emptyList());
        }
        List<Observable<List<Instance>>> chunkObservable = CollectionsExt.chop(instanceIds, AWS_INSTANCE_ID_MAX).stream()
                .map(chunk ->
                        Observable.zip(
                                toObservable(() -> ec2Client.describeInstancesAsync(new DescribeInstancesRequest().withInstanceIds(chunk))),
                                toObservable(() -> autoScalingClient.describeAutoScalingInstancesAsync(new DescribeAutoScalingInstancesRequest().withInstanceIds(chunk))),
                                (ec2Data, autoScalerData) -> toInstances(ec2Data.getReservations(), autoScalerData.getAutoScalingInstances())
                        )
                )
                .collect(Collectors.toList());
        return Observable.merge(chunkObservable, AWS_PARALLELISM)
                .timeout(configuration.getAwsRequestTimeoutMs(), TimeUnit.MILLISECONDS)
                .reduce(new ArrayList<>(), (acc, result) -> {
                    acc.addAll(result);
                    return acc;
                });
    }

    @Override
    public Completable updateCapacity(String instanceGroupId, Optional<Integer> min, Optional<Integer> desired) {
        return toObservable(() -> {
            checkCapacityConstraints(min, desired);
            UpdateAutoScalingGroupRequest request = new UpdateAutoScalingGroupRequest()
                    .withAutoScalingGroupName(instanceGroupId);
            min.ifPresent(request::setMinSize);
            desired.ifPresent(request::setDesiredCapacity);
            return autoScalingClient.updateAutoScalingGroupAsync(request);
        }).toCompletable().timeout(configuration.getAwsRequestTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Observable<List<Either<Boolean, Throwable>>> terminateInstances(String instanceGroup, List<String> instanceIds, boolean shrink) {
        if (instanceIds.isEmpty()) {
            return Observable.just(Collections.emptyList());
        }
        return Observable.fromCallable(() -> {
                    List<List<String>> result = CollectionsExt.chop(instanceIds, AWS_MAX_INSTANCE_DETACH);
                    logger.info("Terminating instances of ASG {} in {} chunk(s) ({})", instanceGroup, result.size(), result);
                    return result;
                }
        ).flatMap(chunks -> ObservableExt.fromWithDelay(
                chunks.stream().map(chunk -> terminateChunk(instanceGroup, chunk, shrink)).collect(Collectors.toList()),
                AWS_OPERATION_DELAY_MS, TimeUnit.MILLISECONDS,
                scheduler
        )).toList(
        ).map(chunkResults -> {
            List<Either<Boolean, Throwable>> instanceResults = new ArrayList<>();
            for (int chunkId = 0; chunkId < chunkResults.size(); chunkId++) {
                Either<Boolean, Throwable> chunkResult = chunkResults.get(chunkId);
                int offset = chunkId * AWS_MAX_INSTANCE_DETACH;
                int last = Math.min(offset + AWS_MAX_INSTANCE_DETACH, instanceIds.size());
                for (int i = offset; i < last; i++) {
                    instanceResults.add(chunkResult);
                }
            }
            return instanceResults;
        });
    }

    /**
     * Internal method used for testing.
     */
    Completable addTagToResource(String resourceId, String tag, String value) {
        List<String> resources = singletonList(resourceId);
        List<Tag> tags = singletonList(new Tag(tag, value));
        return toObservable(() -> ec2Client.createTagsAsync(new CreateTagsRequest(resources, tags))).toCompletable();
    }

    /**
     * This method is used internally by the connector itself for housekeeping.
     */
    Observable<List<Instance>> getTaggedInstances(String tagName) {
        return toObservable(() -> {
            DescribeInstancesRequest request = new DescribeInstancesRequest().withFilters(
                    new Filter().withName("tag-key").withValues(tagName)
            );
            return ec2Client.describeInstancesAsync(request);
        }).flatMap(response -> {
                    List<com.amazonaws.services.ec2.model.Instance> instances =
                            response.getReservations().stream().flatMap(r -> r.getInstances().stream()).collect(Collectors.toList());
                    List<String> instanceIds = instances.stream().map(com.amazonaws.services.ec2.model.Instance::getInstanceId).collect(Collectors.toList());

                    DescribeAutoScalingInstancesRequest asRequest = new DescribeAutoScalingInstancesRequest().withInstanceIds(instanceIds);
                    return toObservable(() -> autoScalingClient.describeAutoScalingInstancesAsync(asRequest))
                            .map(asResponse -> toInstances(response.getReservations(), asResponse.getAutoScalingInstances()));
                }
        );
    }

    /**
     * This method is used internally by the connector itself for housekeeping.
     */
    Completable terminateDetachedInstances(List<String> ids) {
        return toObservable(() -> ec2Client.terminateInstancesAsync(new TerminateInstancesRequest(ids))).toCompletable();
    }

    private Observable<Either<Boolean, Throwable>> terminateChunk(String instanceGroup, List<String> instanceIds, boolean shrink) {
        return ObservableExt.emitError(
                doTag(instanceIds)
                        .concatWith(doDetach(instanceGroup, instanceIds, shrink))
                        .concatWith(doTerminate(instanceIds))
                        .toObservable()
        ).toObservable().map(result -> result.map(Either::<Boolean, Throwable>ofError).orElse(Either.ofValue(true)));
    }

    private Completable doTag(List<String> instanceIds) {
        CreateTagsRequest request = new CreateTagsRequest(
                instanceIds,
                singletonList(new Tag(TAG_TERMINATE, Long.toString(System.currentTimeMillis())))
        );
        return toObservable(() -> ec2Client.createTagsAsync(request))
                .doOnCompleted(() -> logger.info("Tagged instances: {}", instanceIds))
                .doOnError(e -> logger.warn("Failed to tag instances: {}, due to {}", instanceIds, e.getMessage()))
                .toCompletable();
    }

    private Completable doDetach(String instanceGroup, List<String> instanceIds, boolean shrink) {
        DetachInstancesRequest request = new DetachInstancesRequest().withAutoScalingGroupName(instanceGroup)
                .withInstanceIds(instanceIds)
                .withShouldDecrementDesiredCapacity(shrink);
        return toObservable(() -> autoScalingClient.detachInstancesAsync(request))
                .doOnCompleted(() -> logger.info("Detached instances: {}", instanceIds))
                .doOnError(e -> logger.warn("Failed to detach instances: {}, due to {}", instanceIds, e.getMessage()))
                .toCompletable();
    }

    private Completable doTerminate(List<String> instanceIds) {
        TerminateInstancesRequest request = new TerminateInstancesRequest().withInstanceIds(instanceIds);
        return toObservable(() -> ec2Client.terminateInstancesAsync(request))
                .doOnCompleted(() -> logger.info("Terminated instances: {}", instanceIds))
                .doOnError(e -> logger.warn("Failed to terminate instances: {}, due to {}", instanceIds, e.getMessage()))
                .toCompletable();
    }

    private void checkCapacityConstraints(Optional<Integer> minOpt, Optional<Integer> desiredOpt) {
        int effectiveMin = 0;
        if (minOpt.isPresent()) {
            effectiveMin = minOpt.get();
            CloudConnectorException.checkArgument(effectiveMin < 0, "Requested capacity change with invalid min value (< 0)");
        }
        if (desiredOpt.isPresent()) {
            CloudConnectorException.checkArgument(effectiveMin > desiredOpt.get(), "Requested capacity change with invalid desired value (desired < min)");
        }
    }

    private Instance.InstanceState toVmInstanceState(com.amazonaws.services.ec2.model.InstanceState instanceState) {
        if (instanceState == null || instanceState.getCode() == null) {
            return InstanceState.Unknown;
        }
        switch (instanceState.getCode()) {
            case 0: // pending
                return Instance.InstanceState.Starting;
            case 16: // running
                return Instance.InstanceState.Running;
            case 32: // shutting-down
                return InstanceState.Terminating;
            case 48: // terminated
                return Instance.InstanceState.Terminated;
            case 64: // stopping
                return Instance.InstanceState.Stopping;
            case 80: // stopped
                return Instance.InstanceState.Stopped;
        }
        return InstanceState.Unknown;
    }

    private List<InstanceGroup> toInstanceGroups(List<AutoScalingGroup> awsInstanceGroups) {
        return awsInstanceGroups.stream().map(this::toInstanceGroup).collect(Collectors.toList());
    }

    private InstanceGroup toInstanceGroup(AutoScalingGroup awsScalingGroup) {
        boolean isLaunchSuspended = false;
        boolean isTerminateSuspended = false;
        for (SuspendedProcess suspendedProcess : awsScalingGroup.getSuspendedProcesses()) {
            if (suspendedProcess.getProcessName().equals("Launch")) {
                isLaunchSuspended = true;
            } else if (suspendedProcess.getProcessName().equals("Terminate")) {
                isTerminateSuspended = true;
            }
        }
        return new InstanceGroup(
                awsScalingGroup.getAutoScalingGroupName(),
                awsScalingGroup.getLaunchConfigurationName(),
                awsScalingGroup.getMinSize(),
                awsScalingGroup.getDesiredCapacity(),
                awsScalingGroup.getMaxSize(),
                isLaunchSuspended,
                isTerminateSuspended,
                Collections.emptyMap(),
                awsScalingGroup.getInstances().stream().map(com.amazonaws.services.autoscaling.model.Instance::getInstanceId).collect(Collectors.toList())
        );
    }

    private List<InstanceLaunchConfiguration> toVmLaunchConfigurations(List<LaunchConfiguration> awsLaunchConfigurations) {
        return awsLaunchConfigurations.stream().map(this::toVmLaunchConfiguration).collect(Collectors.toList());
    }

    private InstanceLaunchConfiguration toVmLaunchConfiguration(LaunchConfiguration awsLaunchConfiguration) {
        return new InstanceLaunchConfiguration(
                awsLaunchConfiguration.getLaunchConfigurationName(),
                awsLaunchConfiguration.getInstanceType()
        );
    }

    private List<Instance> toInstances(List<Reservation> reservations, List<AutoScalingInstanceDetails> autoScalingInstances) {
        List<com.amazonaws.services.ec2.model.Instance> instances = reservations.stream()
                .flatMap(r -> r.getInstances().stream())
                .collect(Collectors.toList());
        Map<String, AutoScalingInstanceDetails> autoScalerInstanceMap = autoScalingInstances.stream()
                .collect(Collectors.toMap(AutoScalingInstanceDetails::getInstanceId, Function.identity()));

        List<Instance> result = new ArrayList<>();
        for (com.amazonaws.services.ec2.model.Instance instance : instances) {
            result.add(toInstance(instance, Optional.ofNullable(autoScalerInstanceMap.get(instance.getInstanceId()))));
        }
        return result;
    }

    private Instance toInstance(com.amazonaws.services.ec2.model.Instance awsInstance, Optional<AutoScalingInstanceDetails> details) {
        Map<String, String> attributes = CollectionsExt.isNullOrEmpty(awsInstance.getTags())
                ? Collections.emptyMap()
                : awsInstance.getTags().stream().collect(Collectors.toMap(Tag::getKey, Tag::getValue));

        return Instance.newBuilder()
                .withId(awsInstance.getInstanceId())
                .withInstanceGroupId(details.map(AutoScalingInstanceDetails::getAutoScalingGroupName).orElse(UNASSIGNED))
                .withHostname(awsInstance.getPrivateDnsName())
                .withIpAddress(awsInstance.getPrivateIpAddress())
                .withInstanceState(toVmInstanceState(awsInstance.getState()))
                .withAttributes(attributes)
                .withLaunchTime(awsInstance.getLaunchTime().getTime())
                .build();
    }

    private <T> Observable<T> toObservable(Supplier<Future<T>> futureSupplier) {
        return Observable.fromCallable(futureSupplier::get).flatMap(future -> ObservableExt.toObservable(future, scheduler));
    }
}
