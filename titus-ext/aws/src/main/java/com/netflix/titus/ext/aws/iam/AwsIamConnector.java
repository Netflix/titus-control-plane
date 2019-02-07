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
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementAsync;
import com.amazonaws.services.identitymanagement.model.GetRoleRequest;
import com.amazonaws.services.identitymanagement.model.GetRoleResult;
import com.amazonaws.services.identitymanagement.model.NoSuchEntityException;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.connector.cloud.IamConnector;
import com.netflix.titus.api.iam.model.IamRole;
import com.netflix.titus.api.iam.service.IamConnectorException;
import com.netflix.titus.common.util.guice.ProxyType;
import com.netflix.titus.common.util.guice.annotation.ProxyConfiguration;
import com.netflix.titus.ext.aws.AwsConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

@Singleton
@ProxyConfiguration(types = {ProxyType.Logging, ProxyType.Spectator})
public class AwsIamConnector implements IamConnector {
    private static final Logger logger = LoggerFactory.getLogger(AwsIamConnector.class);

    private final AwsConfiguration configuration;
    private final AmazonIdentityManagementAsync iamClient;

    private final Registry registry;
    private final AwsIamConnectorMetrics connectorMetrics;

    @Inject
    public AwsIamConnector(AwsConfiguration configuration,
                           AmazonIdentityManagementAsync iamClient,
                           Registry registry) {
        this.configuration = configuration;
        this.iamClient = iamClient;
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
        long startTime = registry.clock().wallTime();
        return getAwsIamRole(iamRoleName)
                .timeout(Duration.ofMillis(configuration.getAwsRequestTimeoutMs()))
                .map(getRoleResult -> {
                    connectorMetrics.success(AwsIamConnectorMetrics.AwsIamMethods.GetIamRole, startTime);
                    return (IamRole)AwsIamRole.newBuilder()
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
                        return new IamConnectorException(IamConnectorException.ErrorCode.IAM_NOT_FOUND,
                                String.format("Could not find IAM %s: %s", iamRoleName, throwable.getMessage()));
                    }
                    return new IamConnectorException(IamConnectorException.ErrorCode.INTERNAL,
                            String.format("Unable to query IAM %s: %s", iamRoleName, throwable.getMessage()));
                });
    }

    private Mono<GetRoleResult> getAwsIamRole(String iamRoleName) {
        GetRoleRequest request = new GetRoleRequest().withRoleName(iamRoleName);
        return toMono(request, iamClient::getRoleAsync);
    }

    private <REQUEST extends AmazonWebServiceRequest, RESPONSE> Mono<RESPONSE> toMono(
            Supplier<REQUEST> request,
            BiFunction<REQUEST, AsyncHandler<REQUEST, RESPONSE>, Future<RESPONSE>> callFun
    ) {
        return Mono.create(emitter -> {
            AsyncHandler<REQUEST, RESPONSE> asyncHandler = new AsyncHandler<REQUEST, RESPONSE>() {
                @Override
                public void onError(Exception exception) { emitter.error(exception); }

                @Override
                public void onSuccess(REQUEST request, RESPONSE result) { emitter.success(result); }
            };
            Future<RESPONSE> future = callFun.apply(request.get(), asyncHandler);
            emitter.onDispose(() -> {
                if (!future.isCancelled() && !future.isDone()) {
                    future.cancel(true);
                }
            });
        });
    }

    private <REQUEST extends AmazonWebServiceRequest, RESPONSE> Mono<RESPONSE> toMono(
            REQUEST request,
            BiFunction<REQUEST, AsyncHandler<REQUEST, RESPONSE>, Future<RESPONSE>> callFun
    ) {
        Supplier<REQUEST> supplier = () -> request;
        return toMono(supplier, callFun);
    }
}
