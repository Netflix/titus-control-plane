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

package com.netflix.titus.ext.aws;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.autoscaling.AmazonAutoScalingAsync;
import com.amazonaws.services.autoscaling.model.AutoScalingGroup;
import com.amazonaws.services.autoscaling.model.AutoScalingInstanceDetails;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsRequest;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsResult;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingInstancesRequest;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingInstancesResult;
import com.amazonaws.services.autoscaling.model.DescribeLaunchConfigurationsRequest;
import com.amazonaws.services.autoscaling.model.DescribeLaunchConfigurationsResult;
import com.amazonaws.services.autoscaling.model.DetachInstancesRequest;
import com.amazonaws.services.autoscaling.model.DetachInstancesResult;
import com.amazonaws.services.autoscaling.model.LaunchConfiguration;
import com.amazonaws.services.autoscaling.model.SuspendedProcess;
import com.amazonaws.services.autoscaling.model.TagDescription;
import com.amazonaws.services.autoscaling.model.UpdateAutoScalingGroupRequest;
import com.amazonaws.services.autoscaling.model.UpdateAutoScalingGroupResult;
import com.amazonaws.services.ec2.AmazonEC2Async;
import com.amazonaws.services.ec2.model.AmazonEC2Exception;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.CreateTagsResult;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesResult;
import com.google.common.base.Strings;
import com.netflix.titus.api.connector.cloud.CloudConnectorException;
import com.netflix.titus.api.connector.cloud.Instance;
import com.netflix.titus.api.connector.cloud.Instance.InstanceState;
import com.netflix.titus.api.connector.cloud.InstanceCloudConnector;
import com.netflix.titus.api.connector.cloud.InstanceGroup;
import com.netflix.titus.api.connector.cloud.InstanceLaunchConfiguration;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.common.aws.AwsInstanceDescriptor;
import com.netflix.titus.common.aws.AwsInstanceType;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.guice.ProxyType;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.guice.annotation.ProxyConfiguration;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.common.util.tuple.Either;
import com.netflix.titus.common.util.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import rx.Completable;
import rx.Emitter;
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
    static final String TAG_ASG_NAME = "aws:autoscaling:groupName";
    static final String TAG_EC2_AVAILABILITY_ZONE = "aws:ec2:availabilityZone";
    static final String TAG_EC2_INSTANCE_TYPE = "aws:ec2:instanceType";
    static final String TAG_ASG_FILTER_NAME = "tag:" + TAG_ASG_NAME;
    static final String DEFAULT_INSTANCE_TYPE = "unknown";

    private static final int PENDING = 0;
    private static final int RUNNING = 16;
    private static final int SHUTTING_DOWN = 32;
    private static final int TERMINATED = 48;
    private static final int STOPPING = 64;
    private static final int STOPPED = 80;

    public static final String ERROR_INSTANCE_NOT_FOUND = "InvalidInstanceID.NotFound";

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
                                     @Named(DataPlaneAmazonAutoScalingAsyncProvider.NAME) AmazonAutoScalingAsync autoScalingClient) {
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
                request -> {
                    Observable<DescribeAutoScalingGroupsResult> observable = toObservable(request, autoScalingClient::describeAutoScalingGroupsAsync);
                    return observable.map(result -> Pair.of(result.getAutoScalingGroups(), result.getNextToken()));
                }
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

        DescribeAutoScalingGroupsRequest request = new DescribeAutoScalingGroupsRequest().withAutoScalingGroupNames(instanceGroupIds);
        Observable<DescribeAutoScalingGroupsResult> observable = toObservable(request, autoScalingClient::describeAutoScalingGroupsAsync);
        return observable.map(
                response -> toInstanceGroups(response.getAutoScalingGroups())
        ).timeout(configuration.getAwsRequestTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Observable<List<InstanceLaunchConfiguration>> getInstanceLaunchConfiguration(List<String> launchConfigurationIds) {
        if (launchConfigurationIds.isEmpty()) {
            return Observable.just(Collections.emptyList());
        }
        DescribeLaunchConfigurationsRequest request = new DescribeLaunchConfigurationsRequest().withLaunchConfigurationNames(launchConfigurationIds);
        Observable<DescribeLaunchConfigurationsResult> observable = toObservable(request, autoScalingClient::describeLaunchConfigurationsAsync);
        return observable.map(
                response -> toInstanceLaunchConfigurations(response.getLaunchConfigurations())
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
                .map(chunk -> {
                    Observable<DescribeInstancesResult> ec2DescribeObservable = toObservable(new DescribeInstancesRequest().withInstanceIds(chunk), ec2Client::describeInstancesAsync);
                    Observable<DescribeAutoScalingInstancesResult> asgDescribeObservable = toObservable(new DescribeAutoScalingInstancesRequest().withInstanceIds(chunk), autoScalingClient::describeAutoScalingInstancesAsync);
                    return Observable.zip(ec2DescribeObservable, asgDescribeObservable, (ec2Data, autoScalerData) -> toInstances(ec2Data.getReservations(), autoScalerData.getAutoScalingInstances()));
                })
                .collect(Collectors.toList());
        return Observable.merge(chunkObservable, AWS_PARALLELISM)
                .timeout(configuration.getAwsRequestTimeoutMs(), TimeUnit.MILLISECONDS)
                .reduce(new ArrayList<>(), (acc, result) -> {
                    acc.addAll(result);
                    return acc;
                });
    }

    @Override
    public Mono<Instance> getInstance(String instanceId) {
        return Mono.defer(() -> {
            DescribeInstancesRequest req = new DescribeInstancesRequest()
                    .withInstanceIds(instanceId)
                    .withSdkRequestTimeout((int) configuration.getAwsRequestTimeoutMs());
            Mono<DescribeInstancesResult> instancesResult = AwsReactorExt.toMono(req, ec2Client::describeInstancesAsync);
            return instancesResult
                    .filter(isr -> isr.getReservations() != null &&
                            !isr.getReservations().isEmpty() &&
                            !isr.getReservations().get(0).getInstances().isEmpty())
                    .map(isr -> {
                        Reservation reservation = isr.getReservations().get(0);
                        return toInstance(reservation.getInstances().get(0));
                    })
                    .onErrorResume(e -> {
                        if (e instanceof AmazonEC2Exception) {
                            AmazonEC2Exception ec2Exception = (AmazonEC2Exception) e;
                            String errorCode = ec2Exception.getErrorCode();
                            int statusCode = ec2Exception.getStatusCode();
                            // Instance id provided is unknown / invalid
                            if (errorCode != null && errorCode.contains(ERROR_INSTANCE_NOT_FOUND) && statusCode == 400) {
                                logger.info("AWS instance {} NOT FOUND", instanceId);
                                return Mono.empty();
                            }
                        }
                        // Any other exception should bubble up
                        return Mono.error(e);
                    });
        });
    }

    @Override
    public Observable<List<Instance>> getInstancesByInstanceGroupId(String instanceGroupId) {
        if (Strings.isNullOrEmpty(instanceGroupId)) {
            return Observable.just(Collections.emptyList());
        }
        PageCollector<DescribeInstancesRequest, com.amazonaws.services.ec2.model.Instance> pageCollector = new PageCollector<>(
                token -> new DescribeInstancesRequest().withFilters(new Filter().withName(TAG_ASG_FILTER_NAME).withValues(instanceGroupId))
                        .withNextToken(token),
                request -> {
                    Observable<DescribeInstancesResult> observable = toObservable(request, ec2Client::describeInstancesAsync);
                    return observable.map(result -> {
                        List<com.amazonaws.services.ec2.model.Instance> instances = result.getReservations().stream()
                                .flatMap(r -> r.getInstances().stream().filter(instance -> !isTerminal(instance.getState())))
                                .collect(Collectors.toList());
                        return Pair.of(instances, result.getNextToken());
                    });
                }
        );
        return pageCollector.getAll()
                .map(instances -> instances.stream()
                        .filter(instance -> {
                            Optional<Tag> asgTagOptional = instance.getTags().stream().filter(tag -> tag.getKey().equals(TAG_ASG_NAME)).findFirst();
                            return asgTagOptional.isPresent() && asgTagOptional.get().getValue().equals(instanceGroupId);
                        })
                        .map(instance -> toInstance(instance, instanceGroupId)).collect(Collectors.toList()))
                .timeout(configuration.getInstancesByInstanceGroupIdFetchTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Completable updateCapacity(String instanceGroupId, Optional<Integer> min, Optional<Integer> desired) {
        Supplier<UpdateAutoScalingGroupRequest> supplier = () -> {
            logger.info("Updating instance group: {} capacity: min={}, desired={}", instanceGroupId,
                    min.map(Object::toString).orElse("notSet"), desired.map(Object::toString).orElse("notSet"));

            checkCapacityConstraints(min, desired);
            UpdateAutoScalingGroupRequest request = new UpdateAutoScalingGroupRequest()
                    .withAutoScalingGroupName(instanceGroupId);
            min.ifPresent(request::setMinSize);
            desired.ifPresent(request::setDesiredCapacity);
            return request;
        };
        Observable<UpdateAutoScalingGroupResult> observable = toObservable(supplier, autoScalingClient::updateAutoScalingGroupAsync);
        return observable.toCompletable().timeout(configuration.getAwsRequestTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Completable scaleUp(String instanceGroupId, int scaleUpCount) {
        return getInstanceGroups(singletonList(instanceGroupId))
                .map(list -> list.get(0))
                .flatMap(instanceGroup -> {
                    int newDesired = instanceGroup.getDesired() + scaleUpCount;
                    if (newDesired > instanceGroup.getMax()) {
                        return Observable.error(CloudConnectorException.invalidArgument(
                                "Instance group requested desired size %s > max size %s",
                                newDesired, instanceGroup.getMax())
                        );
                    }

                    logger.info("Scaling up instance group {}, by {} instances (desired changed from {} to {})",
                            instanceGroup, scaleUpCount, instanceGroup.getDesired(), newDesired);

                    UpdateAutoScalingGroupRequest request = new UpdateAutoScalingGroupRequest()
                            .withAutoScalingGroupName(instanceGroupId)
                            .withDesiredCapacity(newDesired);
                    Observable<UpdateAutoScalingGroupResult> observable = toObservable(request, autoScalingClient::updateAutoScalingGroupAsync);
                    return observable;
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
        Observable<CreateTagsResult> observable = toObservable(new CreateTagsRequest(resources, tags), ec2Client::createTagsAsync);
        return observable.toCompletable();
    }

    /**
     * This method is used internally by the connector itself for housekeeping.
     */
    Observable<List<Instance>> getTaggedInstances(String tagName) {
        Observable<DescribeInstancesResult> observable = toObservable(
                new DescribeInstancesRequest().withFilters(new Filter().withName("tag-key").withValues(tagName)),
                ec2Client::describeInstancesAsync
        );
        return observable.map(response -> {
                    List<com.amazonaws.services.ec2.model.Instance> instances =
                            response.getReservations().stream()
                                    .flatMap(r -> r.getInstances().stream().filter(instance -> !isTerminal(instance.getState())))
                                    .collect(Collectors.toList());
                    return instances.stream().map(this::toInstance).collect(Collectors.toList());
                }
        );
    }

    /**
     * This method is used internally by the connector itself for housekeeping.
     */
    Completable terminateDetachedInstances(List<String> ids) {
        Observable<TerminateInstancesResult> observable = toObservable(
                new TerminateInstancesRequest(ids),
                ec2Client::terminateInstancesAsync
        );
        return observable.toCompletable();
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
        Observable<CreateTagsResult> observable = toObservable(request, ec2Client::createTagsAsync);
        return observable
                .doOnCompleted(() -> logger.info("Tagged instances: {}", instanceIds))
                .doOnError(e -> logger.warn("Failed to tag instances: {}, due to {}", instanceIds, e.getMessage()))
                .toCompletable();
    }

    private Completable doDetach(String instanceGroup, List<String> instanceIds, boolean shrink) {
        DetachInstancesRequest request = new DetachInstancesRequest().withAutoScalingGroupName(instanceGroup)
                .withInstanceIds(instanceIds)
                .withShouldDecrementDesiredCapacity(shrink);
        Observable<DetachInstancesResult> observable = toObservable(request, autoScalingClient::detachInstancesAsync);
        return observable
                .doOnCompleted(() -> logger.info("Detached instances: {}", instanceIds))
                .doOnError(e -> logger.warn("Failed to detach instances: {}, due to {}", instanceIds, e.getMessage()))
                .toCompletable();
    }

    private Completable doTerminate(List<String> instanceIds) {
        TerminateInstancesRequest request = new TerminateInstancesRequest().withInstanceIds(instanceIds);
        Observable<TerminateInstancesResult> observable = toObservable(request, ec2Client::terminateInstancesAsync);
        return observable
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

    private Instance.InstanceState toInstanceState(com.amazonaws.services.ec2.model.InstanceState instanceState) {
        if (instanceState == null || instanceState.getCode() == null) {
            return InstanceState.Unknown;
        }
        switch (instanceState.getCode()) {
            case PENDING: // pending
                return Instance.InstanceState.Starting;
            case RUNNING: // running
                return Instance.InstanceState.Running;
            case SHUTTING_DOWN: // shutting-down
                return InstanceState.Terminating;
            case TERMINATED: // terminated
                return Instance.InstanceState.Terminated;
            case STOPPING: // stopping
                return Instance.InstanceState.Stopping;
            case STOPPED: // stopped
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
        Map<String, String> attributes = CollectionsExt.isNullOrEmpty(awsScalingGroup.getTags())
                ? Collections.emptyMap()
                : awsScalingGroup.getTags().stream().collect(Collectors.toMap(TagDescription::getKey, TagDescription::getValue));
        return new InstanceGroup(
                awsScalingGroup.getAutoScalingGroupName(),
                awsScalingGroup.getLaunchConfigurationName(),
                DEFAULT_INSTANCE_TYPE,
                awsScalingGroup.getMinSize(),
                awsScalingGroup.getDesiredCapacity(),
                awsScalingGroup.getMaxSize(),
                isLaunchSuspended,
                isTerminateSuspended,
                attributes,
                awsScalingGroup.getInstances().stream().map(com.amazonaws.services.autoscaling.model.Instance::getInstanceId).collect(Collectors.toList())
        );
    }

    private List<InstanceLaunchConfiguration> toInstanceLaunchConfigurations(List<LaunchConfiguration> awsLaunchConfigurations) {
        return awsLaunchConfigurations.stream().map(this::toInstanceLaunchConfiguration).collect(Collectors.toList());
    }

    private InstanceLaunchConfiguration toInstanceLaunchConfiguration(LaunchConfiguration awsLaunchConfiguration) {
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
        Instance instance = toInstance(awsInstance);
        return details.map(d -> instance.toBuilder().withInstanceGroupId(d.getAutoScalingGroupName()).build()).orElse(instance);
    }

    private Instance toInstance(com.amazonaws.services.ec2.model.Instance awsInstance) {
        return toInstance(awsInstance, "unknown");
    }

    private Instance toInstance(com.amazonaws.services.ec2.model.Instance awsInstance, String instanceGroupId) {
        Map<String, String> awsTags = CollectionsExt.isNullOrEmpty(awsInstance.getTags())
                ? Collections.emptyMap()
                : awsInstance.getTags().stream().collect(Collectors.toMap(Tag::getKey, Tag::getValue));

        Map<String, String> defaultAttributes = CollectionsExt.asMap(
                TAG_EC2_AVAILABILITY_ZONE, awsInstance.getPlacement().getAvailabilityZone(),
                TAG_EC2_INSTANCE_TYPE, awsInstance.getInstanceType()
        );

        Map<String, String> attributes = CollectionsExt.merge(awsTags, defaultAttributes);

        return Instance.newBuilder()
                .withId(awsInstance.getInstanceId())
                .withHostname(awsInstance.getPrivateDnsName())
                .withIpAddress(awsInstance.getPrivateIpAddress())
                .withInstanceState(toInstanceState(awsInstance.getState()))
                .withAttributes(attributes)
                .withLaunchTime(awsInstance.getLaunchTime().getTime())
                .withInstanceGroupId(instanceGroupId)
                .build();
    }

    private <REQUEST extends AmazonWebServiceRequest, RESPONSE> Observable<RESPONSE> toObservable(
            Supplier<REQUEST> request,
            BiFunction<REQUEST, AsyncHandler<REQUEST, RESPONSE>, Future<RESPONSE>> callFun
    ) {
        return Observable.create(emitter -> {
            AsyncHandler<REQUEST, RESPONSE> asyncHandler = new AsyncHandler<REQUEST, RESPONSE>() {
                @Override
                public void onError(Exception exception) {
                    emitter.onError(exception);
                }

                @Override
                public void onSuccess(REQUEST request, RESPONSE result) {
                    emitter.onNext(result);
                    emitter.onCompleted();
                }
            };
            Future<RESPONSE> future = callFun.apply(request.get(), asyncHandler);
            emitter.setCancellation(() -> {
                if (!future.isCancelled() && !future.isDone()) {
                    future.cancel(true);
                }
            });
        }, Emitter.BackpressureMode.NONE);
    }

    private <REQUEST extends AmazonWebServiceRequest, RESPONSE> Observable<RESPONSE> toObservable(
            REQUEST request,
            BiFunction<REQUEST, AsyncHandler<REQUEST, RESPONSE>, Future<RESPONSE>> callFun
    ) {
        Supplier<REQUEST> supplier = () -> request;
        return toObservable(supplier, callFun);
    }

    private boolean isTerminal(com.amazonaws.services.ec2.model.InstanceState instanceState) {
        Integer code = instanceState.getCode();
        return code == STOPPED || code == TERMINATED;
    }
}
