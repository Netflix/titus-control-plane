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

package com.netflix.titus.master.appscale.endpoint.v3.grpc;

import java.util.concurrent.ThreadLocalRandom;

import com.google.protobuf.BoolValue;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.netflix.titus.api.appscale.model.PolicyType;
import com.netflix.titus.grpc.protogen.AlarmConfiguration;
import com.netflix.titus.grpc.protogen.CustomizedMetricSpecification;
import com.netflix.titus.grpc.protogen.MetricDimension;
import com.netflix.titus.grpc.protogen.PutPolicyRequest;
import com.netflix.titus.grpc.protogen.ScalingPolicy;
import com.netflix.titus.grpc.protogen.ScalingPolicyID;
import com.netflix.titus.grpc.protogen.StepAdjustments;
import com.netflix.titus.grpc.protogen.StepScalingPolicy;
import com.netflix.titus.grpc.protogen.StepScalingPolicyDescriptor;
import com.netflix.titus.grpc.protogen.TargetTrackingPolicyDescriptor;
import com.netflix.titus.grpc.protogen.UpdatePolicyRequest;

public class AutoScalingTestUtils {

    public static UpdatePolicyRequest generateUpdateTargetTrackingPolicyRequest(String policyRefId, double targetValue) {
        ScalingPolicy scalingPolicy = generateTargetPolicy();
        TargetTrackingPolicyDescriptor targetPolicyDescriptor = scalingPolicy.getTargetPolicyDescriptor();
        TargetTrackingPolicyDescriptor targetPolicyWithUpdatedValue = targetPolicyDescriptor.toBuilder().setTargetValue(DoubleValue.newBuilder().setValue(targetValue).build()).build();
        ScalingPolicy scalingPolicyTobeUpdated = scalingPolicy.toBuilder().setTargetPolicyDescriptor(targetPolicyWithUpdatedValue).build();
        UpdatePolicyRequest updatePolicyRequest = UpdatePolicyRequest.newBuilder().setPolicyId(ScalingPolicyID.newBuilder().setId(policyRefId).build())
                .setScalingPolicy(scalingPolicyTobeUpdated).build();
        return updatePolicyRequest;
    }

    public static UpdatePolicyRequest generateUpdateStepScalingPolicyRequest(String policyRefId, double threshold) {
        ScalingPolicy scalingPolicy = generateStepPolicy();
        AlarmConfiguration alarmConfig = scalingPolicy.getStepPolicyDescriptor().getAlarmConfig().toBuilder().setThreshold(DoubleValue.newBuilder().setValue(threshold).build()).build();
        StepScalingPolicyDescriptor stepScalingPolicyDescriptor = scalingPolicy.getStepPolicyDescriptor().toBuilder().setAlarmConfig(alarmConfig).build();
        ScalingPolicy scalingPolicyToBeUpdated = scalingPolicy.toBuilder().setStepPolicyDescriptor(stepScalingPolicyDescriptor).build();
        UpdatePolicyRequest updatePolicyRequest = UpdatePolicyRequest.newBuilder().setPolicyId(ScalingPolicyID.newBuilder().setId(policyRefId).build())
                .setScalingPolicy(scalingPolicyToBeUpdated).build();
        return updatePolicyRequest;
    }

    public static PutPolicyRequest generatePutPolicyRequest(String jobId, PolicyType policyType) {
        ScalingPolicy scalingPolicy;
        if (policyType == PolicyType.StepScaling) {
            scalingPolicy = generateStepPolicy();
        } else {
            scalingPolicy = generateTargetPolicy();
        }
        return PutPolicyRequest.newBuilder()
                .setJobId(jobId)
                .setScalingPolicy(scalingPolicy)
                .build();
    }

    public static ScalingPolicy generateTargetPolicy() {
        CustomizedMetricSpecification customizedMetricSpec = CustomizedMetricSpecification.newBuilder()
                .addDimensions(MetricDimension.newBuilder()
                        .setName("testName")
                        .setValue("testValue")
                        .build())
                .setMetricName("testMetric")
                .setNamespace("NFLX/EPIC")
                .setStatistic(AlarmConfiguration.Statistic.Sum)
                .setMetricName("peanuts")
                .build();

        TargetTrackingPolicyDescriptor targetTrackingPolicyDescriptor = TargetTrackingPolicyDescriptor.newBuilder()
                .setTargetValue(DoubleValue.newBuilder()
                        .setValue(ThreadLocalRandom.current().nextDouble())
                        .build())
                .setScaleInCooldownSec(Int32Value.newBuilder()
                        .setValue(ThreadLocalRandom.current().nextInt())
                        .build())
                .setScaleOutCooldownSec(Int32Value.newBuilder()
                        .setValue(ThreadLocalRandom.current().nextInt())
                        .build())
                .setDisableScaleIn(BoolValue.newBuilder()
                        .setValue(false)
                        .build())
                .setCustomizedMetricSpecification(customizedMetricSpec)
                .build();
        return ScalingPolicy.newBuilder().setTargetPolicyDescriptor(targetTrackingPolicyDescriptor).build();
    }

    /**
     * Builds a random scaling policy for use with tests.
     *
     * @return
     */
    public static ScalingPolicy generateStepPolicy() {
        // TODO(Andrew L): Add target tracking support
        AlarmConfiguration alarmConfig = AlarmConfiguration.newBuilder()
                .setActionsEnabled(BoolValue.newBuilder()
                        .setValue(ThreadLocalRandom.current().nextBoolean())
                        .build())
                .setComparisonOperator(AlarmConfiguration.ComparisonOperator.GreaterThanThreshold)
                .setEvaluationPeriods(Int32Value.newBuilder()
                        .setValue(ThreadLocalRandom.current().nextInt())
                        .build())
                .setPeriodSec(Int32Value.newBuilder()
                        .setValue(ThreadLocalRandom.current().nextInt())
                        .build())
                .setThreshold(DoubleValue.newBuilder()
                        .setValue(ThreadLocalRandom.current().nextDouble())
                        .build())
                .setMetricNamespace("NFLX/EPIC")
                .setMetricName("Metric-" + ThreadLocalRandom.current().nextInt())
                .setStatistic(AlarmConfiguration.Statistic.Sum)
                .build();

        StepScalingPolicy stepScalingPolicy = StepScalingPolicy.newBuilder()
                .setAdjustmentType(StepScalingPolicy.AdjustmentType.ChangeInCapacity)
                .setCooldownSec(Int32Value.newBuilder()
                        .setValue(ThreadLocalRandom.current().nextInt())
                        .build())
                .setMinAdjustmentMagnitude(Int64Value.newBuilder()
                        .setValue(ThreadLocalRandom.current().nextLong())
                        .build())
                .setMetricAggregationType(StepScalingPolicy.MetricAggregationType.Maximum)
                .addStepAdjustments(StepAdjustments.newBuilder()
                        .setMetricIntervalLowerBound(DoubleValue.newBuilder()
                                .setValue(ThreadLocalRandom.current().nextDouble())
                                .build())
                        .setMetricIntervalUpperBound(DoubleValue.newBuilder()
                                .setValue(ThreadLocalRandom.current().nextDouble())
                                .build())
                        .setScalingAdjustment(Int32Value.newBuilder()
                                .setValue(ThreadLocalRandom.current().nextInt())
                                .build()))
                .build();

        StepScalingPolicyDescriptor stepScalingPolicyDescriptor = StepScalingPolicyDescriptor.newBuilder()
                .setAlarmConfig(alarmConfig)
                .setScalingPolicy(stepScalingPolicy)
                .build();
        return ScalingPolicy.newBuilder().setStepPolicyDescriptor(stepScalingPolicyDescriptor).build();
    }
}
