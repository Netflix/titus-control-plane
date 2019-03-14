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

package com.netflix.titus.ext.aws.iam;

import java.time.Duration;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.amazonaws.services.identitymanagement.AmazonIdentityManagementAsync;
import com.amazonaws.services.identitymanagement.model.GetRoleRequest;
import com.amazonaws.services.identitymanagement.model.GetRoleResult;
import com.amazonaws.services.identitymanagement.model.NoSuchEntityException;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.connector.cloud.IamConnector;
import com.netflix.titus.api.iam.model.IamRole;
import com.netflix.titus.api.iam.service.IamConnectorException;
import com.netflix.titus.common.util.cache.Cache;
import com.netflix.titus.common.util.cache.Caches;
import com.netflix.titus.common.util.guice.ProxyType;
import com.netflix.titus.common.util.guice.annotation.ProxyConfiguration;
import com.netflix.titus.ext.aws.AwsConfiguration;
import com.netflix.titus.ext.aws.AwsReactorExt;
import reactor.core.publisher.Mono;

@Singleton
@ProxyConfiguration(types = {ProxyType.Logging, ProxyType.Spectator})
public class AwsIamConnector implements IamConnector {

    private static final long MAX_CACHE_SIZE = 5_000;

    private final AwsConfiguration configuration;
    private final AmazonIdentityManagementAsync iamClient;

    private final Registry registry;
    private final AwsIamConnectorMetrics connectorMetrics;

    private final Cache<String, IamRole> cache;

    @Inject
    public AwsIamConnector(AwsConfiguration configuration,
                           AmazonIdentityManagementAsync iamClient,
                           Registry registry) {
        this.configuration = configuration;
        this.iamClient = iamClient;
        this.cache = Caches.instrumentedCacheWithMaxSize(
                MAX_CACHE_SIZE,
                Duration.ofMillis(configuration.getIamRoleCacheTimeoutMs()),
                AwsIamConnectorMetrics.METRICS_ROOT + ".iamRoleCache",
                registry
        );
        this.registry = registry;
        this.connectorMetrics = new AwsIamConnectorMetrics(registry);
    }

    @PreDestroy
    public void shutdown() {
        iamClient.shutdown();
    }

    /**
     * Gets an IAM role from AWS. The iamRoleName provided is expected to be an AWS friendly Role name:
     * (https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_identifiers.html#identifiers-friendly-names)
     * not an ARN. The AWS request will fail if an friendly name is not used.
     */
    @Override
    public Mono<IamRole> getIamRole(String iamRoleName) {
        return Mono.defer(() -> {
            IamRole iamRole = cache.getIfPresent(iamRoleName);
            if (iamRole != null) {
                return Mono.just(iamRole);
            }
            return getIamRoleFromAws(iamRoleName).doOnNext(fetchedIamRole -> cache.put(iamRoleName, fetchedIamRole));
        });
    }

    private Mono<IamRole> getIamRoleFromAws(String iamRoleName) {
        long startTime = registry.clock().wallTime();
        return getAwsIamRole(iamRoleName)
                .timeout(Duration.ofMillis(configuration.getAwsRequestTimeoutMs()))
                .map(getRoleResult -> {
                            connectorMetrics.success(AwsIamConnectorMetrics.AwsIamMethods.GetIamRole, startTime);
                            return IamRole.newBuilder()
                                    .withRoleId(getRoleResult.getRole().getRoleId())
                                    .withRoleName(getRoleResult.getRole().getRoleName())
                                    .withResourceName(getRoleResult.getRole().getArn())
                                    .withPolicyDoc(getRoleResult.getRole().getAssumeRolePolicyDocument())
                                    .build();
                        }
                )
                .onErrorMap(throwable -> {
                    connectorMetrics.failure(AwsIamConnectorMetrics.AwsIamMethods.GetIamRole, throwable, startTime);
                    if (throwable instanceof NoSuchEntityException) {
                        return IamConnectorException.iamRoleNotFound(iamRoleName);
                    }
                    return IamConnectorException.iamRoleUnexpectedError(iamRoleName, throwable.getMessage());
                });
    }

    @Override
    public Mono<Void> canIamAssume(String iamRoleName, String assumeResourceName) {
        return getIamRole(iamRoleName)
                .flatMap(iamRole -> {
                    if (AwsIamUtil.canAssume(iamRole, assumeResourceName)) {
                        return Mono.empty();
                    }
                    return Mono.error(IamConnectorException.iamRoleCannotAssume(iamRole.getRoleName(), assumeResourceName));
                });
    }

    private Mono<GetRoleResult> getAwsIamRole(String iamRoleName) {
        GetRoleRequest request = new GetRoleRequest().withRoleName(iamRoleName);
        return AwsReactorExt.toMono(request, iamClient::getRoleAsync);
    }
}
