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

package io.netflix.titus.master.appscale.endpoint.v3.grpc;

import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.BoolValue;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.netflix.titus.grpc.protogen.*;
import io.netflix.titus.api.appscale.model.AutoScalingPolicy;
import io.netflix.titus.api.appscale.model.ComparisonOperator;
import io.netflix.titus.api.appscale.model.MetricAggregationType;
import io.netflix.titus.api.appscale.model.PolicyStatus;
import io.netflix.titus.api.appscale.model.Statistic;
import io.netflix.titus.api.appscale.model.StepAdjustment;
import io.netflix.titus.api.appscale.model.StepAdjustmentType;
import io.netflix.titus.api.appscale.model.StepScalingPolicyConfiguration;
import io.netflix.titus.api.appscale.model.TargetTrackingPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.netflix.titus.api.appscale.model.PolicyType.StepScaling;
import static io.netflix.titus.api.appscale.model.PolicyType.TargetTrackingScaling;

/**
 * Collection of functions to convert policy models from internal to gRPC formats.
 */
public final class GrpcModelConverters {
    private static Logger log = LoggerFactory.getLogger(GrpcModelConverters.class);

    public static ScalingPolicyID toScalingPolicyId(String policyId) {
        return ScalingPolicyID.newBuilder().setId(policyId).build();
    }

    public static ScalingPolicyResult toScalingPolicyResult(AutoScalingPolicy autoScalingPolicy) {
        ScalingPolicy scalingPolicy = toScalingPolicy(autoScalingPolicy);

        ScalingPolicyStatus scalingPolicyStatus = toScalingPolicyStatus(autoScalingPolicy.getStatus(),
                autoScalingPolicy.getStatusMessage());

        return ScalingPolicyResult.newBuilder()
                .setJobId(autoScalingPolicy.getJobId())
                .setId(ScalingPolicyID.newBuilder().setId(autoScalingPolicy.getRefId()).build())
                .setPolicyState(scalingPolicyStatus)
                .setScalingPolicy(scalingPolicy)
                .build();
    }

    private static ScalingPolicy toScalingPolicy(AutoScalingPolicy autoScalingPolicy) {
        ScalingPolicy.Builder scalingPolicyBuilder = ScalingPolicy.newBuilder();

        if (autoScalingPolicy.getPolicyConfiguration().getPolicyType() == StepScaling) {
            AlarmConfiguration alarmConfiguration =
                    toAlarmConfiguration(autoScalingPolicy.getPolicyConfiguration().getAlarmConfiguration());
            StepScalingPolicy stepScalingPolicy =
                    toStepScalingPolicy(autoScalingPolicy.getPolicyConfiguration().getStepScalingPolicyConfiguration());

            StepScalingPolicyDescriptor stepScalingPolicyDescriptor =
                    StepScalingPolicyDescriptor.newBuilder()
                            .setAlarmConfig(alarmConfiguration)
                            .setScalingPolicy(stepScalingPolicy)
                            .build();
            scalingPolicyBuilder
                    .setStepPolicyDescriptor(stepScalingPolicyDescriptor);
        } else if (autoScalingPolicy.getPolicyConfiguration().getPolicyType() == TargetTrackingScaling) {
            TargetTrackingPolicyDescriptor targetTrackingPolicyDesc =
                    toTargetTrackingPolicyDescriptor(autoScalingPolicy.getPolicyConfiguration().getTargetTrackingPolicy());

            scalingPolicyBuilder
                    .setTargetPolicyDescriptor(targetTrackingPolicyDesc);
        } else {
            throw new IllegalArgumentException("Invalid AutoScalingPolicyType value "
                    + autoScalingPolicy.getPolicyConfiguration().getPolicyType());
        }

        return scalingPolicyBuilder.build();
    }

    private static AlarmConfiguration toAlarmConfiguration(io.netflix.titus.api.appscale.model.AlarmConfiguration alarmConfiguration) {
        AlarmConfiguration.Builder alarmConfigBuilder = AlarmConfiguration.newBuilder();
        alarmConfiguration.getActionsEnabled().ifPresent(
                actionsEnabled ->
                        alarmConfigBuilder.setActionsEnabled(BoolValue.newBuilder()
                                .setValue(actionsEnabled)
                                .build())
        );

        AlarmConfiguration.ComparisonOperator comparisonOperator =
                toComparisonOperator(alarmConfiguration.getComparisonOperator());
        AlarmConfiguration.Statistic statistic =
                toStatistic(alarmConfiguration.getStatistic());
        return AlarmConfiguration.newBuilder()
                .setComparisonOperator(comparisonOperator)
                .setEvaluationPeriods(Int32Value.newBuilder()
                        .setValue(alarmConfiguration.getEvaluationPeriods())
                        .build())
                .setPeriodSec(Int32Value.newBuilder()
                        .setValue(alarmConfiguration.getPeriodSec())
                        .build())
                .setThreshold(DoubleValue.newBuilder()
                        .setValue(alarmConfiguration.getThreshold())
                        .build())
                .setMetricNamespace(alarmConfiguration.getMetricNamespace())
                .setMetricName(alarmConfiguration.getMetricName())
                .setStatistic(statistic)
                .build();
    }

    private static StepScalingPolicy toStepScalingPolicy(StepScalingPolicyConfiguration stepScalingPolicyConfiguration) {
        StepScalingPolicy.Builder stepScalingPolicyBuilder = StepScalingPolicy.newBuilder();

        stepScalingPolicyConfiguration.getCoolDownSec().ifPresent(
                coolDown ->
                    stepScalingPolicyBuilder.setCooldownSec(Int32Value.newBuilder().setValue(coolDown).build())
        );
        stepScalingPolicyConfiguration.getMetricAggregationType().ifPresent(
                metricAggregationType ->
                        stepScalingPolicyBuilder.setMetricAggregationType(toMetricAggregationType(metricAggregationType))
        );
        stepScalingPolicyConfiguration.getAdjustmentType().ifPresent(
                stepAdjustmentType ->
                        stepScalingPolicyBuilder.setAdjustmentType(toAdjustmentType(stepAdjustmentType))
        );
        stepScalingPolicyConfiguration.getMinAdjustmentMagnitude().ifPresent(
                minAdjustmentMagnitude ->
                        stepScalingPolicyBuilder.setMinAdjustmentMagnitude(
                                Int64Value.newBuilder()
                                        .setValue(minAdjustmentMagnitude)
                                        .build())
        );
        stepScalingPolicyBuilder.addAllStepAdjustments(toStepAdjustmentsList(stepScalingPolicyConfiguration.getSteps()));

        return stepScalingPolicyBuilder
                .build();
    }

    private static TargetTrackingPolicyDescriptor toTargetTrackingPolicyDescriptor(TargetTrackingPolicy targetTrackingPolicy) {
        TargetTrackingPolicyDescriptor.Builder targetTrackingPolicyDescBuilder = TargetTrackingPolicyDescriptor.newBuilder();
        targetTrackingPolicyDescBuilder.setTargetValue(DoubleValue.newBuilder()
                .setValue(targetTrackingPolicy.getTargetValue())
                .build());

        targetTrackingPolicy.getScaleOutCooldownSec().ifPresent(
                scaleOutCoolDownSec -> targetTrackingPolicyDescBuilder.setScaleOutCooldownSec(
                        Int32Value.newBuilder().setValue(scaleOutCoolDownSec).build())
        );
        targetTrackingPolicy.getScaleInCooldownSec().ifPresent(
                scaleInCoolDownSec -> targetTrackingPolicyDescBuilder.setScaleInCooldownSec(
                        Int32Value.newBuilder().setValue(scaleInCoolDownSec).build())
        );
        targetTrackingPolicy.getDisableScaleIn().ifPresent(
                disableScaleIn -> targetTrackingPolicyDescBuilder.setDisableScaleIn(
                        BoolValue.newBuilder().setValue(disableScaleIn).build())
        );
        targetTrackingPolicy.getPredefinedMetricSpecification().ifPresent(
                predefinedMetricSpecification ->
                        targetTrackingPolicyDescBuilder.setPredefinedMetricSpecification(
                                toPredefinedMetricSpecification(targetTrackingPolicy.getPredefinedMetricSpecification().get()))
        );
        targetTrackingPolicy.getCustomizedMetricSpecification().ifPresent(
                customizedMetricSpecification ->
                        targetTrackingPolicyDescBuilder.setCustomizedMetricSpecification(
                                toCustomizedMetricSpecification(targetTrackingPolicy.getCustomizedMetricSpecification().get()))
        );

        return targetTrackingPolicyDescBuilder.build();
    }

    private static CustomizedMetricSpecification toCustomizedMetricSpecification(io.netflix.titus.api.appscale.model.CustomizedMetricSpecification customizedMetricSpec) {
        CustomizedMetricSpecification.Builder customizedMetricSpecBuilder = CustomizedMetricSpecification.newBuilder();
        customizedMetricSpecBuilder
                .addAllDimensions(toMetricDimensionList(customizedMetricSpec.getMetricDimensionList()))
                .setMetricName(customizedMetricSpec.getMetricName())
                .setNamespace(customizedMetricSpec.getNamespace())
                .setStatistic(toStatistic(customizedMetricSpec.getStatistic()));
        customizedMetricSpec.getUnit().ifPresent(
                unit -> customizedMetricSpecBuilder.setUnit(customizedMetricSpec.getUnit().get())
        );

        return customizedMetricSpecBuilder.build();
    }

    private static List<MetricDimension> toMetricDimensionList(List<io.netflix.titus.api.appscale.model.MetricDimension> metricDimensionList) {
        List<MetricDimension> metricDimensionGrpcList = new ArrayList<>();
        metricDimensionList.forEach((metricDimension) -> {
            metricDimensionGrpcList.add(toMetricDimension(metricDimension));
        });
        return metricDimensionGrpcList;
    }

    private static MetricDimension toMetricDimension(io.netflix.titus.api.appscale.model.MetricDimension metricDimension) {
        return MetricDimension.newBuilder()
                .setName(metricDimension.getName())
                .setValue(metricDimension.getValue())
                .build();
    }

    private static PredefinedMetricSpecification toPredefinedMetricSpecification(io.netflix.titus.api.appscale.model.PredefinedMetricSpecification predefinedMetricSpec) {
        PredefinedMetricSpecification.Builder predefinedMetricSpecBuilder = PredefinedMetricSpecification.newBuilder();
        predefinedMetricSpecBuilder.setPredefinedMetricType(predefinedMetricSpec.getPredefinedMetricType());

        predefinedMetricSpec.getResourceLabel().ifPresent(
                resourceLabel ->
                        predefinedMetricSpecBuilder.setResourceLabel(predefinedMetricSpec.getResourceLabel().get())
        );

        return predefinedMetricSpecBuilder.build();
    }

    private static ScalingPolicyStatus toScalingPolicyStatus(PolicyStatus policyStatus, String statusMessage) {
        ScalingPolicyStatus.ScalingPolicyState policyState;
        switch (policyStatus) {
            case Pending:
                policyState = ScalingPolicyStatus.ScalingPolicyState.Pending;
                break;
            case Applied:
                policyState = ScalingPolicyStatus.ScalingPolicyState.Applied;
                break;
            case Deleting:
                policyState = ScalingPolicyStatus.ScalingPolicyState.Deleting;
                break;
            case Deleted:
                policyState = ScalingPolicyStatus.ScalingPolicyState.Deleted;
                break;
            default:
                throw new IllegalArgumentException("Invalid PolicyStatus value " + policyStatus);
        }
        if (statusMessage == null) {
            statusMessage = "";
        }

        return ScalingPolicyStatus.newBuilder()
                .setState(policyState)
                .setPendingReason(statusMessage)
                .build();
    }

    private static StepScalingPolicy.AdjustmentType toAdjustmentType(StepAdjustmentType stepAdjustmentType) {
        StepScalingPolicy.AdjustmentType adjustmentType;
        switch (stepAdjustmentType) {
            case ChangeInCapacity:
                adjustmentType = StepScalingPolicy.AdjustmentType.ChangeInCapacity;
                break;
            case PercentChangeInCapacity:
                adjustmentType = StepScalingPolicy.AdjustmentType.PercentChangeInCapacity;
                break;
            case ExactCapacity:
                adjustmentType = StepScalingPolicy.AdjustmentType.ExactCapacity;
                break;
            default:
                  throw new IllegalArgumentException("Invalid StepAdjustmentType value " + stepAdjustmentType);
        }
        return adjustmentType;
    }

    private static StepScalingPolicy.MetricAggregationType toMetricAggregationType(MetricAggregationType metricAggregationType) {
        StepScalingPolicy.MetricAggregationType metricAggregationTypeGrpc;
        switch (metricAggregationType) {
            case Average:
                metricAggregationTypeGrpc = StepScalingPolicy.MetricAggregationType.Average;
                break;
            case Maximum:
                metricAggregationTypeGrpc = StepScalingPolicy.MetricAggregationType.Maximum;
                break;
            case Minimum:
                metricAggregationTypeGrpc = StepScalingPolicy.MetricAggregationType.Minimum;
                break;
            default:
                throw new IllegalArgumentException("Invalid MetricAggregationType value " + metricAggregationType);
        }
        return metricAggregationTypeGrpc;
    }

    private static StepAdjustments toStepAdjustments(StepAdjustment stepAdjustment) {
        StepAdjustments.Builder stepAdjustmentsBuilder = StepAdjustments.newBuilder()
                .setScalingAdjustment(Int32Value.newBuilder()
                        .setValue(stepAdjustment.getScalingAdjustment())
                        .build());
        stepAdjustment.getMetricIntervalLowerBound().ifPresent(
                metricIntervalLowerBound ->
                        stepAdjustmentsBuilder.setMetricIntervalLowerBound(DoubleValue.newBuilder()
                                .setValue(metricIntervalLowerBound)
                                .build())
        );
        stepAdjustment.getMetricIntervalUpperBound().ifPresent(
                metricIntervalUpperBound ->
                        stepAdjustmentsBuilder.setMetricIntervalUpperBound(DoubleValue.newBuilder()
                                .setValue(metricIntervalUpperBound)
                                .build())
        );

        return stepAdjustmentsBuilder.build();
    }

    private static List<StepAdjustments> toStepAdjustmentsList(List<StepAdjustment> stepAdjustmentList) {
        List<StepAdjustments> stepAdjustmentGrpcList = new ArrayList<>();
        stepAdjustmentList.forEach((stepAdjustment) -> {
            stepAdjustmentGrpcList.add(toStepAdjustments(stepAdjustment));
        });
        return stepAdjustmentGrpcList;
    }

    private static AlarmConfiguration.ComparisonOperator toComparisonOperator(ComparisonOperator comparisonOperator) {
        AlarmConfiguration.ComparisonOperator comparisonOperatorGrpc;
        switch (comparisonOperator) {
            case GreaterThanOrEqualToThreshold:
                comparisonOperatorGrpc = AlarmConfiguration.ComparisonOperator.GreaterThanOrEqualToThreshold;
                break;
            case GreaterThanThreshold:
                comparisonOperatorGrpc = AlarmConfiguration.ComparisonOperator.GreaterThanThreshold;
                break;
            case LessThanOrEqualToThreshold:
                comparisonOperatorGrpc = AlarmConfiguration.ComparisonOperator.LessThanOrEqualToThreshold;
                break;
            case LessThanThreshold:
                comparisonOperatorGrpc = AlarmConfiguration.ComparisonOperator.LessThanThreshold;
                break;
            default:
                throw new IllegalArgumentException("Invalid ComparisonOperator value " + comparisonOperator);
        }
        return comparisonOperatorGrpc;
    }

    private static AlarmConfiguration.Statistic toStatistic(Statistic statistic) {
        AlarmConfiguration.Statistic statisticGrpc;
        switch (statistic) {
            case SampleCount:
                statisticGrpc = AlarmConfiguration.Statistic.SampleCount;
                break;
            case Sum:
                statisticGrpc = AlarmConfiguration.Statistic.Sum;
                break;
            case Average:
                statisticGrpc = AlarmConfiguration.Statistic.Average;
                break;
            case Maximum:
                statisticGrpc = AlarmConfiguration.Statistic.Maximum;
                break;
            case Minimum:
                statisticGrpc = AlarmConfiguration.Statistic.Minimum;
                break;
            default:
                throw new IllegalArgumentException("Invalid Statistic value " + statistic);
        }
        return statisticGrpc;
    }
}
