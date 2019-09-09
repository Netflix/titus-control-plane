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
import javax.inject.Named;
import javax.inject.Singleton;

import com.amazonaws.services.identitymanagement.AmazonIdentityManagementAsync;
import com.amazonaws.services.identitymanagement.model.GetRoleRequest;
import com.amazonaws.services.identitymanagement.model.GetRoleResult;
import com.amazonaws.services.identitymanagement.model.NoSuchEntityException;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceAsync;
import com.amazonaws.services.securitytoken.model.AWSSecurityTokenServiceException;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.connector.cloud.IamConnector;
import com.netflix.titus.api.iam.model.IamRole;
import com.netflix.titus.api.iam.service.IamConnectorException;
import com.netflix.titus.common.util.cache.Cache;
import com.netflix.titus.common.util.cache.Caches;
import com.netflix.titus.common.util.guice.ProxyType;
import com.netflix.titus.common.util.guice.annotation.ProxyConfiguration;
import com.netflix.titus.common.util.spectator.IamConnectorMetrics;
import com.netflix.titus.ext.aws.AwsConfiguration;
import com.netflix.titus.ext.aws.AwsReactorExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

@Singleton
@ProxyConfiguration(types = {ProxyType.Logging, ProxyType.Spectator})
public class AwsIamConnector implements IamConnector {

    private static final Logger logger = LoggerFactory.getLogger(AwsIamConnector.class);

    public static final String STS_AGENT = "stsAgent";

    private static final long MAX_CACHE_SIZE = 5_000;

    /**
     * As documented here {@link AssumeRoleRequest}.
     */
    private static final int MIN_ASSUMED_ROLE_DURATION_SEC = 900;

    private final AwsConfiguration configuration;
    private final AmazonIdentityManagementAsync iamClient;
    private final AWSSecurityTokenServiceAsync stsAgentClient;

    private final Registry registry;
    private final IamConnectorMetrics connectorMetrics;

    private final Cache<String, IamRole> cache;
    private final Cache<String, Boolean> canAssumeCache;

    @Inject
    public AwsIamConnector(AwsConfiguration configuration,
                           AmazonIdentityManagementAsync iamClient,
                           @Named(STS_AGENT) AWSSecurityTokenServiceAsync stsAgentClient,
                           Registry registry) {
        this.configuration = configuration;
        this.iamClient = iamClient;
        this.stsAgentClient = stsAgentClient;
        this.cache = Caches.instrumentedCacheWithMaxSize(
                MAX_CACHE_SIZE,
                Duration.ofMillis(configuration.getIamRoleCacheTimeoutMs()),
                IamConnectorMetrics.METRICS_ROOT + ".awsIamRoleCache",
                registry
        );
        this.canAssumeCache = Caches.instrumentedCacheWithMaxSize(
                MAX_CACHE_SIZE,
                Duration.ofMillis(configuration.getIamRoleCacheTimeoutMs()),
                IamConnectorMetrics.METRICS_ROOT + ".awsCanAssumeIamRoleCache",
                registry
        );
        this.registry = registry;
        this.connectorMetrics = new IamConnectorMetrics(AwsIamConnector.class, registry);
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
                            connectorMetrics.success(IamConnectorMetrics.IamMethods.GetIamRole, startTime);
                            return IamRole.newBuilder()
                                    .withRoleId(getRoleResult.getRole().getRoleId())
                                    .withRoleName(getRoleResult.getRole().getRoleName())
                                    .withResourceName(getRoleResult.getRole().getArn())
                                    .withPolicyDoc(getRoleResult.getRole().getAssumeRolePolicyDocument())
                                    .build();
                        }
                )
                .onErrorMap(throwable -> {
                    // Remap to specific Exception if we got rate limited
                    if (throwable.getMessage().contains("Rate exceeded")) {
                        throwable = new AwsIamRateLimitException(throwable);
                    }
                    connectorMetrics.failure(IamConnectorMetrics.IamMethods.GetIamRole, throwable, startTime);
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

    @Override
    public Mono<Void> canAgentAssume(String iamRoleName) {
        return Mono.defer(() -> {
            long startTime = registry.clock().wallTime();

            // Check cache first
            if (Boolean.TRUE.equals(canAssumeCache.getIfPresent(iamRoleName))) {
                return Mono.empty();
            }

            // Must call AWS STS service
            return AwsReactorExt
                    .<AssumeRoleRequest, AssumeRoleResult>toMono(
                            () -> new AssumeRoleRequest()
                                    .withRoleSessionName("titusIamRoleValidation")
                                    .withRoleArn(iamRoleName)
                                    .withDurationSeconds(MIN_ASSUMED_ROLE_DURATION_SEC),
                            stsAgentClient::assumeRoleAsync
                    )
                    .flatMap(response -> {
                        logger.debug("Assumed into: {}", iamRoleName);
                        canAssumeCache.put(iamRoleName, true);
                        connectorMetrics.success(IamConnectorMetrics.IamMethods.CanAgentAssume, startTime);
                        return Mono.<Void>empty();
                    })
                    .onErrorMap(error -> {
                        logger.debug("Error: {}", error.getMessage());
                        connectorMetrics.failure(IamConnectorMetrics.IamMethods.CanAgentAssume, error, startTime);

                        String errorCode = ((AWSSecurityTokenServiceException) error).getErrorCode();
                        if ("AccessDenied".equals(errorCode)) {
                            // STS service returns access denied error with no additional clues. To get more insight we
                            // would have to make a call to IAM service, but this would require access to all client accounts.
                            return IamConnectorException.iamRoleCannotAssume(iamRoleName, configuration.getDataPlaneAgentRoleArn());
                        }
                        return IamConnectorException.iamRoleUnexpectedError(iamRoleName, error.getMessage());
                    });
        });
    }

    private Mono<GetRoleResult> getAwsIamRole(String iamRoleName) {
        GetRoleRequest request = new GetRoleRequest().withRoleName(iamRoleName);
        return AwsReactorExt.toMono(request, iamClient::getRoleAsync);
    }
}
