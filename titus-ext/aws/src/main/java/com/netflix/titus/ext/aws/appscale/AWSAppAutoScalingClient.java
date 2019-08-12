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

package com.netflix.titus.ext.aws.appscale;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.applicationautoscaling.AWSApplicationAutoScalingAsync;
import com.amazonaws.services.applicationautoscaling.AWSApplicationAutoScalingAsyncClientBuilder;
import com.amazonaws.services.applicationautoscaling.model.CustomizedMetricSpecification;
import com.amazonaws.services.applicationautoscaling.model.DeleteScalingPolicyRequest;
import com.amazonaws.services.applicationautoscaling.model.DeleteScalingPolicyResult;
import com.amazonaws.services.applicationautoscaling.model.DeregisterScalableTargetRequest;
import com.amazonaws.services.applicationautoscaling.model.DeregisterScalableTargetResult;
import com.amazonaws.services.applicationautoscaling.model.DescribeScalableTargetsRequest;
import com.amazonaws.services.applicationautoscaling.model.DescribeScalableTargetsResult;
import com.amazonaws.services.applicationautoscaling.model.MetricDimension;
import com.amazonaws.services.applicationautoscaling.model.ObjectNotFoundException;
import com.amazonaws.services.applicationautoscaling.model.PutScalingPolicyRequest;
import com.amazonaws.services.applicationautoscaling.model.PutScalingPolicyResult;
import com.amazonaws.services.applicationautoscaling.model.RegisterScalableTargetRequest;
import com.amazonaws.services.applicationautoscaling.model.RegisterScalableTargetResult;
import com.amazonaws.services.applicationautoscaling.model.ScalableTarget;
import com.amazonaws.services.applicationautoscaling.model.StepAdjustment;
import com.amazonaws.services.applicationautoscaling.model.StepScalingPolicyConfiguration;
import com.amazonaws.services.applicationautoscaling.model.TargetTrackingScalingPolicyConfiguration;
import com.amazonaws.services.applicationautoscaling.model.ValidationException;
import com.google.common.annotations.VisibleForTesting;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.appscale.model.AutoScalableTarget;
import com.netflix.titus.api.appscale.model.PolicyConfiguration;
import com.netflix.titus.api.appscale.model.PolicyType;
import com.netflix.titus.api.appscale.model.TargetTrackingPolicy;
import com.netflix.titus.api.appscale.service.AutoScalePolicyException;
import com.netflix.titus.api.connector.cloud.AppAutoScalingClient;
import com.netflix.titus.ext.aws.RetryWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Emitter;
import rx.Observable;

import static com.netflix.titus.ext.aws.appscale.AWSAppAutoScalingUtil.buildScalingPolicyName;


@Singleton
public class AWSAppAutoScalingClient implements AppAutoScalingClient {
    private static Logger logger = LoggerFactory.getLogger(AWSAppAutoScalingClient.class);
    public final static String SERVICE_NAMESPACE = "custom-resource";
    // AWS requires this field be set to this specific value for all application-autoscaling calls.
    public final static String SCALABLE_DIMENSION = "custom-resource:ResourceType:Property";

    private final AWSApplicationAutoScalingAsync awsAppAutoScalingClientAsync;
    private final AWSAppScalingConfig awsAppScalingConfig;
    private final AWSAppAutoScalingMetrics awsAppAutoScalingMetrics;

    @Inject
    public AWSAppAutoScalingClient(AWSCredentialsProvider awsCredentialsProvider,
                                   AWSAppScalingConfig awsAppScalingConfig,
                                   Registry registry) {
        this(AWSApplicationAutoScalingAsyncClientBuilder.standard()
                        .withCredentials(awsCredentialsProvider)
                        .withRegion(awsAppScalingConfig.getRegion()).build(),
                awsAppScalingConfig, registry);
    }

    @VisibleForTesting
    AWSAppAutoScalingClient(AWSApplicationAutoScalingAsync awsAppAutoScalingClientAsync, AWSAppScalingConfig awsAppScalingConfig, Registry registry) {
        this.awsAppAutoScalingClientAsync = awsAppAutoScalingClientAsync;
        this.awsAppScalingConfig = awsAppScalingConfig;
        this.awsAppAutoScalingMetrics = new AWSAppAutoScalingMetrics(registry);
    }

    @Override
    public Completable createScalableTarget(String jobId, int minCapacity, int maxCapacity) {
        RegisterScalableTargetRequest registerScalableTargetRequest = new RegisterScalableTargetRequest();
        registerScalableTargetRequest.setMinCapacity(minCapacity);
        registerScalableTargetRequest.setMaxCapacity(maxCapacity);
        registerScalableTargetRequest.setResourceId(AWSAppAutoScalingUtil.buildGatewayResourceId(jobId,
                awsAppScalingConfig.getAWSGatewayEndpointPrefix(),
                awsAppScalingConfig.getRegion(),
                awsAppScalingConfig.getAWSGatewayEndpointTargetStage()));
        registerScalableTargetRequest.setServiceNamespace(SERVICE_NAMESPACE);
        registerScalableTargetRequest.setScalableDimension(SCALABLE_DIMENSION);
        logger.info("RegisterScalableTargetRequest {}", registerScalableTargetRequest);

        return RetryWrapper.wrapWithExponentialRetry(String.format("createScalableTarget for job %s", jobId),
                Observable.create(emitter -> awsAppAutoScalingClientAsync.registerScalableTargetAsync(registerScalableTargetRequest, new AsyncHandler<RegisterScalableTargetRequest, RegisterScalableTargetResult>() {
                    @Override
                    public void onError(Exception exception) {
                        logger.error("Register scalable target exception for {} - {}", jobId, exception.getMessage());
                        awsAppAutoScalingMetrics.registerAwsCreateTargetError(exception);
                        emitter.onError(exception);
                    }

                    @Override
                    public void onSuccess(RegisterScalableTargetRequest request, RegisterScalableTargetResult registerScalableTargetResult) {
                        int httpStatusCode = registerScalableTargetResult.getSdkHttpMetadata().getHttpStatusCode();
                        logger.info("Registered scalable target for success {} - status {}", jobId, httpStatusCode);
                        awsAppAutoScalingMetrics.registerAwsCreateTargetSuccess();
                        emitter.onCompleted();
                    }
                }), Emitter.BackpressureMode.NONE)).toCompletable();
    }

    @Override
    public Observable<AutoScalableTarget> getScalableTargetsForJob(String jobId) {
        DescribeScalableTargetsRequest describeScalableTargetsRequest = new DescribeScalableTargetsRequest();
        describeScalableTargetsRequest.setServiceNamespace(SERVICE_NAMESPACE);
        describeScalableTargetsRequest.setScalableDimension(SCALABLE_DIMENSION);
        describeScalableTargetsRequest.setResourceIds(Collections.singletonList(
                AWSAppAutoScalingUtil.buildGatewayResourceId(jobId,
                        awsAppScalingConfig.getAWSGatewayEndpointPrefix(),
                        awsAppScalingConfig.getRegion(),
                        awsAppScalingConfig.getAWSGatewayEndpointTargetStage())));

        return RetryWrapper.wrapWithExponentialRetry(String.format("getScalableTargetsForJob for job %s", jobId),
                Observable.create(emitter -> awsAppAutoScalingClientAsync.describeScalableTargetsAsync(describeScalableTargetsRequest,
                        new AsyncHandler<DescribeScalableTargetsRequest, DescribeScalableTargetsResult>() {
                            @Override
                            public void onError(Exception exception) {
                                logger.error("Get scalable target exception for {} - {}", jobId, exception.getMessage());
                                awsAppAutoScalingMetrics.registerAwsGetTargetError(exception);
                                emitter.onError(exception);
                            }

                            @Override
                            public void onSuccess(DescribeScalableTargetsRequest request, DescribeScalableTargetsResult describeScalableTargetsResult) {
                                awsAppAutoScalingMetrics.registerAwsGetTargetSuccess();
                                List<ScalableTarget> scalableTargets = describeScalableTargetsResult.getScalableTargets();
                                scalableTargets.stream()
                                        .map(AWSAppAutoScalingUtil::toAutoScalableTarget)
                                        .forEach(emitter::onNext);
                                emitter.onCompleted();
                            }
                        }), Emitter.BackpressureMode.NONE));
    }

    @Override
    public Observable<String> createOrUpdateScalingPolicy(String policyRefId, String jobId, PolicyConfiguration policyConfiguration) {
        PutScalingPolicyRequest putScalingPolicyRequest = new PutScalingPolicyRequest();

        putScalingPolicyRequest.setPolicyName(buildScalingPolicyName(policyRefId, jobId));

        putScalingPolicyRequest.setPolicyType(policyConfiguration.getPolicyType().name());
        putScalingPolicyRequest.setResourceId(
                AWSAppAutoScalingUtil.buildGatewayResourceId(jobId,
                        awsAppScalingConfig.getAWSGatewayEndpointPrefix(),
                        awsAppScalingConfig.getRegion(),
                        awsAppScalingConfig.getAWSGatewayEndpointTargetStage()));
        putScalingPolicyRequest.setServiceNamespace(SERVICE_NAMESPACE);
        putScalingPolicyRequest.setScalableDimension(SCALABLE_DIMENSION);

        if (policyConfiguration.getPolicyType() == PolicyType.StepScaling) {
            StepScalingPolicyConfiguration stepScalingPolicyConfiguration = new StepScalingPolicyConfiguration();
            if (policyConfiguration.getStepScalingPolicyConfiguration().getMetricAggregationType().isPresent()) {
                stepScalingPolicyConfiguration.setMetricAggregationType(policyConfiguration.getStepScalingPolicyConfiguration().getMetricAggregationType().get().name());
            }

            if (policyConfiguration.getStepScalingPolicyConfiguration().getCoolDownSec().isPresent()) {
                stepScalingPolicyConfiguration.setCooldown(policyConfiguration.getStepScalingPolicyConfiguration().getCoolDownSec().get());
            }

            List<com.netflix.titus.api.appscale.model.StepAdjustment> steps = policyConfiguration.getStepScalingPolicyConfiguration().getSteps();
            List<StepAdjustment> stepAdjustments = steps.stream()
                    .map(step -> {
                        StepAdjustment stepAdjustment = new StepAdjustment();
                        if (step.getMetricIntervalUpperBound() != null && step.getMetricIntervalUpperBound().isPresent()) {
                            stepAdjustment.setMetricIntervalUpperBound(step.getMetricIntervalUpperBound().get());
                        }
                        if (step.getMetricIntervalLowerBound() != null && step.getMetricIntervalLowerBound().isPresent()) {
                            stepAdjustment.setMetricIntervalLowerBound(step.getMetricIntervalLowerBound().get());
                        }
                        stepAdjustment.setScalingAdjustment(step.getScalingAdjustment());
                        return stepAdjustment;
                    })
                    .collect(Collectors.toList());

            stepScalingPolicyConfiguration.setStepAdjustments(stepAdjustments);
            if (policyConfiguration.getStepScalingPolicyConfiguration().getAdjustmentType().isPresent()) {
                stepScalingPolicyConfiguration.setAdjustmentType(policyConfiguration.getStepScalingPolicyConfiguration().getAdjustmentType().get().name());
            }

            putScalingPolicyRequest.setStepScalingPolicyConfiguration(stepScalingPolicyConfiguration);
        } else if (policyConfiguration.getPolicyType() == PolicyType.TargetTrackingScaling) {
            TargetTrackingScalingPolicyConfiguration targetTrackingConfigAws = new TargetTrackingScalingPolicyConfiguration();
            TargetTrackingPolicy targetTrackingPolicyInt = policyConfiguration.getTargetTrackingPolicy();

            targetTrackingConfigAws.setTargetValue(targetTrackingPolicyInt.getTargetValue());
            if (targetTrackingPolicyInt.getDisableScaleIn().isPresent()) {
                targetTrackingConfigAws.setDisableScaleIn(targetTrackingPolicyInt.getDisableScaleIn().get());
            }
            if (targetTrackingPolicyInt.getScaleInCooldownSec().isPresent()) {
                targetTrackingConfigAws.setScaleInCooldown(targetTrackingPolicyInt.getScaleInCooldownSec().get());
            }
            if (targetTrackingPolicyInt.getScaleOutCooldownSec().isPresent()) {
                targetTrackingConfigAws.setScaleOutCooldown(targetTrackingPolicyInt.getScaleOutCooldownSec().get());
            }
            if (targetTrackingPolicyInt.getCustomizedMetricSpecification().isPresent()) {
                com.netflix.titus.api.appscale.model.CustomizedMetricSpecification customizedMetricSpecInt =
                        targetTrackingPolicyInt.getCustomizedMetricSpecification().get();
                CustomizedMetricSpecification customizedMetricSpecAws = new CustomizedMetricSpecification();

                customizedMetricSpecAws.setDimensions(customizedMetricSpecInt.getMetricDimensionList()
                        .stream()
                        .map(metricDimensionInt -> {
                            MetricDimension metricDimensionAws = new MetricDimension()
                                    .withName(metricDimensionInt.getName())
                                    .withValue(metricDimensionInt.getValue());
                            return metricDimensionAws;
                        })
                        .collect(Collectors.toList()));
                customizedMetricSpecAws.setMetricName(customizedMetricSpecInt.getMetricName());
                customizedMetricSpecAws.setNamespace(customizedMetricSpecInt.getNamespace());
                customizedMetricSpecAws.setStatistic(customizedMetricSpecInt.getStatistic().name());
                if (customizedMetricSpecInt.getUnit().isPresent()) {
                    customizedMetricSpecAws.setUnit(customizedMetricSpecInt.getUnit().get());
                }

                targetTrackingConfigAws.setCustomizedMetricSpecification(customizedMetricSpecAws);
            }

            putScalingPolicyRequest.setTargetTrackingScalingPolicyConfiguration(targetTrackingConfigAws);
        } else {
            return Observable.error(new UnsupportedOperationException("Scaling policy type " + policyConfiguration.getPolicyType().name() + " is not supported."));
        }

        return RetryWrapper.wrapWithExponentialRetry(String.format("createOrUpdateScalingPolicy %s for job %s", policyRefId, jobId),
                Observable.create(emitter -> awsAppAutoScalingClientAsync.putScalingPolicyAsync(putScalingPolicyRequest, new AsyncHandler<PutScalingPolicyRequest, PutScalingPolicyResult>() {
                    @Override
                    public void onError(Exception exception) {
                        logger.error("Exception creating scaling policy ", exception);
                        awsAppAutoScalingMetrics.registerAwsCreatePolicyError(exception);
                        if (exception instanceof ValidationException) {
                            emitter.onError(AutoScalePolicyException.invalidScalingPolicy(policyRefId, exception.getMessage()));
                        } else {
                            emitter.onError(AutoScalePolicyException.errorCreatingPolicy(policyRefId, exception.getMessage()));
                        }
                    }

                    @Override
                    public void onSuccess(PutScalingPolicyRequest request, PutScalingPolicyResult putScalingPolicyResult) {
                        String policyARN = putScalingPolicyResult.getPolicyARN();
                        logger.info("New Scaling policy {} created {} for Job {}", request, policyARN, jobId);
                        awsAppAutoScalingMetrics.registerAwsCreatePolicySuccess();
                        emitter.onNext(policyARN);
                        emitter.onCompleted();
                    }
                }), Emitter.BackpressureMode.NONE));
    }

    @Override
    public Completable deleteScalableTarget(String jobId) {
        DeregisterScalableTargetRequest deRegisterRequest = new DeregisterScalableTargetRequest();
        deRegisterRequest.setResourceId(
                AWSAppAutoScalingUtil.buildGatewayResourceId(jobId,
                        awsAppScalingConfig.getAWSGatewayEndpointPrefix(),
                        awsAppScalingConfig.getRegion(),
                        awsAppScalingConfig.getAWSGatewayEndpointTargetStage()));
        deRegisterRequest.setServiceNamespace(SERVICE_NAMESPACE);
        deRegisterRequest.setScalableDimension(SCALABLE_DIMENSION);

        return RetryWrapper.wrapWithExponentialRetry(String.format("deleteScalableTarget for job %s", jobId),
                Observable.create(emitter -> awsAppAutoScalingClientAsync.deregisterScalableTargetAsync(deRegisterRequest, new AsyncHandler<DeregisterScalableTargetRequest, DeregisterScalableTargetResult>() {
                    @Override
                    public void onError(Exception exception) {
                        if (exception instanceof ObjectNotFoundException) {
                            logger.info("Scalable target does not exist anymore for job {}", jobId);
                            emitter.onCompleted();
                        } else {
                            logger.error("Deregister scalable target exception {} - {}", jobId, exception.getMessage());
                            awsAppAutoScalingMetrics.registerAwsDeleteTargetError(exception);
                            emitter.onError(exception);
                        }
                    }

                    @Override
                    public void onSuccess(DeregisterScalableTargetRequest request, DeregisterScalableTargetResult deregisterScalableTargetResult) {
                        int httpStatusCode = deregisterScalableTargetResult.getSdkHttpMetadata().getHttpStatusCode();
                        logger.info("De-registered scalable target for {}, status {}", jobId, httpStatusCode);
                        awsAppAutoScalingMetrics.registerAwsDeleteTargetSuccess();
                        emitter.onCompleted();
                    }
                }), Emitter.BackpressureMode.NONE)).toCompletable();
    }

    @Override
    public Completable deleteScalingPolicy(String policyRefId, String jobId) {
        DeleteScalingPolicyRequest deleteScalingPolicyRequest = new DeleteScalingPolicyRequest();
        deleteScalingPolicyRequest.setResourceId(
                AWSAppAutoScalingUtil.buildGatewayResourceId(jobId,
                        awsAppScalingConfig.getAWSGatewayEndpointPrefix(),
                        awsAppScalingConfig.getRegion(),
                        awsAppScalingConfig.getAWSGatewayEndpointTargetStage()));
        deleteScalingPolicyRequest.setServiceNamespace(SERVICE_NAMESPACE);
        deleteScalingPolicyRequest.setScalableDimension(SCALABLE_DIMENSION);
        deleteScalingPolicyRequest.setPolicyName(buildScalingPolicyName(policyRefId, jobId));

        return RetryWrapper.wrapWithExponentialRetry(String.format("deleteScalingPolicy %s for job %s", policyRefId, jobId),
                Observable.create(emitter -> awsAppAutoScalingClientAsync.deleteScalingPolicyAsync(deleteScalingPolicyRequest, new AsyncHandler<DeleteScalingPolicyRequest, DeleteScalingPolicyResult>() {
                    @Override
                    public void onError(Exception exception) {
                        if (exception instanceof ObjectNotFoundException) {
                            logger.info("Scaling policy does not exist anymore for job/policyRefId {}/{}", jobId, policyRefId);
                            emitter.onCompleted();
                        } else {
                            logger.error("Delete scaling policy exception {} - {}", jobId, exception.getMessage());
                            awsAppAutoScalingMetrics.registerAwsDeletePolicyError(exception);
                            emitter.onError(AutoScalePolicyException.errorDeletingPolicy(policyRefId, exception.getMessage()));
                        }
                    }

                    @Override
                    public void onSuccess(DeleteScalingPolicyRequest request, DeleteScalingPolicyResult deleteScalingPolicyResult) {
                        int httpStatusCode = deleteScalingPolicyResult.getSdkHttpMetadata().getHttpStatusCode();
                        logger.info("Deleted scaling policy for job/policyRefId {}/{}, status - {}", jobId, policyRefId, httpStatusCode);
                        awsAppAutoScalingMetrics.registerAwsDeletePolicySuccess();
                        emitter.onCompleted();

                    }
                }), Emitter.BackpressureMode.NONE)).toCompletable();
    }
}
