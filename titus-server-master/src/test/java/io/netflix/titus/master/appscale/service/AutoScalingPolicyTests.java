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

package io.netflix.titus.master.appscale.service;

import java.util.Arrays;
import java.util.List;
import java.util.function.BooleanSupplier;

import io.netflix.titus.api.appscale.model.AlarmConfiguration;
import io.netflix.titus.api.appscale.model.AutoScalableTarget;
import io.netflix.titus.api.appscale.model.AutoScalingPolicy;
import io.netflix.titus.api.appscale.model.ComparisonOperator;
import io.netflix.titus.api.appscale.model.CustomizedMetricSpecification;
import io.netflix.titus.api.appscale.model.MetricAggregationType;
import io.netflix.titus.api.appscale.model.PolicyConfiguration;
import io.netflix.titus.api.appscale.model.PolicyStatus;
import io.netflix.titus.api.appscale.model.PolicyType;
import io.netflix.titus.api.appscale.model.Statistic;
import io.netflix.titus.api.appscale.model.StepAdjustment;
import io.netflix.titus.api.appscale.model.StepAdjustmentType;
import io.netflix.titus.api.appscale.model.StepScalingPolicyConfiguration;
import io.netflix.titus.api.appscale.model.TargetTrackingPolicy;
import io.netflix.titus.api.connector.cloud.AppAutoScalingClient;
import io.netflix.titus.api.connector.cloud.CloudAlarmClient;
import rx.Completable;
import rx.Observable;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class AutoScalingPolicyTests {
    public static class MockAlarmClient implements CloudAlarmClient {
        int numOfAlarmsCreated = 0;

        @Override
        public Observable<String> createOrUpdateAlarm(String policyRefId, String jobId, AlarmConfiguration alarmConfiguration,
                                                      String autoScalingGroup, List<String> actions) {
            numOfAlarmsCreated++;
            return Observable.just("alarmARM");
        }

        @Override
        public Completable deleteAlarm(String jobId, String alarmName) {
            numOfAlarmsCreated--;
            return Completable.complete();
        }

        public int getNumOfAlarmsCreated() {
            return numOfAlarmsCreated;
        }
    }

    public static class MockAppAutoScalingClient implements AppAutoScalingClient {
        int numScalableTargets = 0;
        int numPolicies = 0;

        public int getNumScalableTargets() {
            return numScalableTargets;
        }

        public int getNumPolicies() {
            return numPolicies;
        }

        @Override
        public Completable createScalableTarget(String jobId, int minCapacity, int maxCapacity) {
            numScalableTargets++;
            return Completable.complete();
        }

        @Override
        public Observable<String> createOrUpdateScalingPolicy(String policyRefId, String jobId, PolicyConfiguration policyConfiguration) {
            numPolicies++;
            return Observable.just("policyARN");
        }

        @Override
        public Completable deleteScalableTarget(String jobId) {
            numScalableTargets--;
            return Completable.complete();
        }

        @Override
        public Completable deleteScalingPolicy(String policyRefId, String jobId) {
            numPolicies--;
            return Completable.complete();
        }

        @Override
        public Observable<AutoScalableTarget> getScalableTargetsForJob(String jobId) {
            return Observable.empty();
        }
    }

    public static AutoScalingPolicy buildTargetTrackingPolicy(String jobId) {
        CustomizedMetricSpecification customizedMetricSpec = CustomizedMetricSpecification.newBuilder()
                .withNamespace("foobar")
                .withNamespace("NFLX/EPIC")
                .withStatistic(Statistic.Sum)
                .withUnit("Seconds")
                .build();

        TargetTrackingPolicy targetTrackingPolicy = TargetTrackingPolicy.newBuilder()
                .withDisableScaleIn(false)
                .withScaleInCooldownSec(10)
                .withScaleOutCooldownSec(5)
                .withCustomizedMetricSpecification(customizedMetricSpec)
                .build();

        PolicyConfiguration policyConfiguration = PolicyConfiguration.newBuilder()
                .withTargetTrackingPolicy(targetTrackingPolicy)
                .withPolicyType(PolicyType.TargetTrackingScaling)
                .build();

        AutoScalingPolicy autoScalingPolicy = AutoScalingPolicy.newBuilder()
                .withPolicyConfiguration(policyConfiguration)
                .withStatus(PolicyStatus.Pending)
                .withStatusMessage("ICE-ed by AWS")
                .withJobId(jobId)
                .build();
        return autoScalingPolicy;
    }

    public static AutoScalingPolicy buildStepScalingPolicy(String jobId) {
        AlarmConfiguration alarmConfiguration = AlarmConfiguration.newBuilder()
                .withActionsEnabled(true)
                .withAutoScalingGroupName("anyscale-amit-v000")
                .withComparisonOperator(ComparisonOperator.GreaterThanThreshold)
                .withEvaluationPeriods(1)
                .withPeriodSec(60)
                .withMetricName("CPUUtilization")
                .withMetricNamespace("AWS/EC2")
                .withName(jobId)
                .withStatistic(Statistic.Average)
                .build();

        StepAdjustment stepAdjustment = StepAdjustment.newBuilder()
                .withMetricIntervalLowerBound(0)
                .withScalingAdjustment(1)
                .build();

        StepScalingPolicyConfiguration stepScalingPolicyConfiguration = StepScalingPolicyConfiguration.newBuilder()
                .withAdjustmentType(StepAdjustmentType.ChangeInCapacity)
                .withCoolDownSec(60)
                .withMetricAggregatorType(MetricAggregationType.Average)
                .withMinAdjustmentMagnitude(1)
                .withSteps(Arrays.asList(stepAdjustment))
                .build();

        PolicyConfiguration policyConfiguration = PolicyConfiguration.newBuilder()
                .withAlarmConfiguration(alarmConfiguration)
                .withStepScalingPolicyConfiguration(stepScalingPolicyConfiguration)
                .withPolicyType(PolicyType.StepScaling)
                .withName(jobId)
                .build();


        AutoScalingPolicy autoScalingPolicy = AutoScalingPolicy.newBuilder()
                .withPolicyConfiguration(policyConfiguration)
                .withStatus(PolicyStatus.Pending)
                .withStatusMessage("ICE-ed by AWS")
                .withJobId(jobId)
                .build();
        return autoScalingPolicy;
    }


    public static AppScaleManagerConfiguration mockAppScaleManagerConfiguration() {
        AppScaleManagerConfiguration appScaleManagerConfiguration = mock(AppScaleManagerConfiguration.class);
        when(appScaleManagerConfiguration.getReconcileFinishedJobsIntervalMins()).thenReturn(1L);
        when(appScaleManagerConfiguration.getReconcileTargetsIntervalMins()).thenReturn(1L);
        when(appScaleManagerConfiguration.getStoreInitTimeoutSeconds()).thenReturn(5L);
        return appScaleManagerConfiguration;
    }

    public static boolean waitForCondition(BooleanSupplier booleanSupplier) throws Exception {
        int maxChecks = 200;
        int i = 0;
        while (true) {
            if (booleanSupplier.getAsBoolean()) {
                return true;
            } else {
                Thread.sleep(100);
            }
            if (i++ >= maxChecks) {
                return true;
            }
        }
    }

}
