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

package com.netflix.titus.ext.aws.loadbalancer;

import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancingAsync;
import com.amazonaws.services.elasticloadbalancingv2.model.*;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.connector.cloud.CloudConnectorException;
import com.netflix.titus.api.connector.cloud.LoadBalancerConnector;
import com.netflix.titus.api.loadbalancer.service.LoadBalancerException;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.guice.ProxyType;
import com.netflix.titus.common.util.guice.annotation.ProxyConfiguration;
import com.netflix.titus.ext.aws.AwsObservableExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;
import rx.Scheduler;
import rx.Single;
import rx.schedulers.Schedulers;

@Singleton
@ProxyConfiguration(types = {ProxyType.Logging, ProxyType.Spectator})
public class AwsLoadBalancerConnector implements LoadBalancerConnector {
    private static final Logger logger = LoggerFactory.getLogger(AwsLoadBalancerConnector.class);
    private static final String AWS_IP_TARGET_TYPE = "ip";

    private final AmazonElasticLoadBalancingAsync client;
    private final Registry registry;
    private final Scheduler scheduler;

    private AwsLoadBalancerConnectorMetrics connectorMetrics;

    @Inject
    public AwsLoadBalancerConnector(AmazonElasticLoadBalancingAsync client, Registry registry) {
        this(client, Schedulers.computation(), registry);
    }

    private AwsLoadBalancerConnector(AmazonElasticLoadBalancingAsync client, Scheduler scheduler, Registry registry) {
        this.client = client;
        this.scheduler = scheduler;
        this.registry = registry;
        this.connectorMetrics = new AwsLoadBalancerConnectorMetrics(registry);
    }

    @Override
    public Completable registerAll(String loadBalancerId, Set<String> ipAddresses) {
        if (CollectionsExt.isNullOrEmpty(ipAddresses)) {
            return Completable.complete();
        }

        // TODO: retry logic
        // TODO: handle partial failures in the batch
        // TODO: timeouts

        final Set<TargetDescription> targetDescriptions = ipAddresses.stream().map(
                ipAddress -> new TargetDescription().withId(ipAddress)
        ).collect(Collectors.toSet());
        final RegisterTargetsRequest request = new RegisterTargetsRequest()
                .withTargetGroupArn(loadBalancerId)
                .withTargets(targetDescriptions);

        long startTime = registry.clock().wallTime();
        // force observeOn(scheduler) since the callback will be called from the AWS SDK threadpool
        return AwsObservableExt.asyncActionCompletable(factory -> client.registerTargetsAsync(request, factory.handler(
                (req, resp) -> {
                    logger.debug("Registered targets {}", resp);
                    connectorMetrics.success(AwsLoadBalancerConnectorMetrics.AwsLoadBalancerMethods.RegisterTargets, startTime);
                },
                (t) -> {
                    logger.error("Error registering targets on " + loadBalancerId, t);
                    connectorMetrics.failure(AwsLoadBalancerConnectorMetrics.AwsLoadBalancerMethods.RegisterTargets, t, startTime);
                }
        ))).observeOn(scheduler);
    }

    @Override
    public Completable deregisterAll(String loadBalancerId, Set<String> ipAddresses) {
        if (CollectionsExt.isNullOrEmpty(ipAddresses)) {
            return Completable.complete();
        }

        // TODO: retry logic
        // TODO: handle partial failures in the batch
        // TODO: timeouts

        final DeregisterTargetsRequest request = new DeregisterTargetsRequest()
                .withTargetGroupArn(loadBalancerId)
                .withTargets(ipAddresses.stream().map(
                        ipAddress -> new TargetDescription().withId(ipAddress)
                ).collect(Collectors.toSet()));

        long startTime = registry.clock().wallTime();
        // force observeOn(scheduler) since the callback will be called from the AWS SDK threadpool
        return AwsObservableExt.asyncActionCompletable(supplier -> client.deregisterTargetsAsync(request, supplier.handler(
                (req, resp) -> {
                    logger.debug("Deregistered targets {}", resp);
                    connectorMetrics.success(AwsLoadBalancerConnectorMetrics.AwsLoadBalancerMethods.DeregisterTargets, startTime);
                },
                (t) -> {
                    logger.error("Error deregistering targets on " + loadBalancerId, t);
                    connectorMetrics.failure(AwsLoadBalancerConnectorMetrics.AwsLoadBalancerMethods.DeregisterTargets, t, startTime);
                }
        ))).observeOn(scheduler);
    }

    @Override
    public Completable isValid(String loadBalancerId) {
        final DescribeTargetGroupsRequest request = new DescribeTargetGroupsRequest()
                .withTargetGroupArns(loadBalancerId);

        long startTime = registry.clock().wallTime();
        Single<DescribeTargetGroupsResult> resultSingle = AwsObservableExt.asyncActionSingle(supplier -> client.describeTargetGroupsAsync(request, supplier.handler()));
        return resultSingle
                .observeOn(scheduler)
                .doOnError(throwable -> {
                    connectorMetrics.failure(AwsLoadBalancerConnectorMetrics.AwsLoadBalancerMethods.DescribeTargetGroups, throwable, startTime);
                })
                .flatMapObservable(result -> {
                    connectorMetrics.success(AwsLoadBalancerConnectorMetrics.AwsLoadBalancerMethods.DescribeTargetGroups, startTime);
                    return Observable.from(result.getTargetGroups());
                })
                .flatMap(targetGroup -> {
                    if (targetGroup.getTargetType().equals(AWS_IP_TARGET_TYPE)) {
                        return Observable.empty();
                    }
                    return Observable.error(CloudConnectorException.invalidArgument(String.format("Target group %s is NOT of required type %s", targetGroup.getTargetGroupArn(), AWS_IP_TARGET_TYPE)));
                })
                .toCompletable();
    }

    @Override
    public Single<Set<String>> getRegisteredIps(String loadBalancerId) {
        final DescribeTargetHealthRequest request = new DescribeTargetHealthRequest()
                .withTargetGroupArn(loadBalancerId);

        long startTime = registry.clock().wallTime();
        Single<DescribeTargetHealthResult> asyncResult = AwsObservableExt.asyncActionSingle(
                factory -> client.describeTargetHealthAsync(request, factory.handler())
        );

        return asyncResult
                .observeOn(scheduler)
                .onErrorResumeNext(throwable -> {
                    connectorMetrics.failure(AwsLoadBalancerConnectorMetrics.AwsLoadBalancerMethods.DescribeTargetHealth, throwable, startTime);
                    // In order to conditionally handle this exception elsewhere without requiring a dependency on AWS
                    // libraries we wrap this Exception.
                    if (throwable instanceof TargetGroupNotFoundException) {
                        throwable = LoadBalancerException.targetGroupNotFound(loadBalancerId, throwable);
                    }

                    return Single.error(throwable);
                })
                .map(result -> {
                    connectorMetrics.success(AwsLoadBalancerConnectorMetrics.AwsLoadBalancerMethods.DescribeTargetHealth, startTime);
                    return ipsFromResult(result);
                });
    }

    private Set<String> ipsFromResult(DescribeTargetHealthResult result) {
        return result.getTargetHealthDescriptions().stream()
                .map(description -> description.getTarget().getId())
                .collect(Collectors.toSet());
    }
}
