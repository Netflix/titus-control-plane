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

package io.netflix.titus.ext.cassandra.store;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.netflix.spectator.api.DefaultRegistry;
import io.netflix.titus.api.appscale.model.AlarmConfiguration;
import io.netflix.titus.api.appscale.model.AutoScalingPolicy;
import io.netflix.titus.api.appscale.model.ComparisonOperator;
import io.netflix.titus.api.appscale.model.MetricAggregationType;
import io.netflix.titus.api.appscale.model.PolicyConfiguration;
import io.netflix.titus.api.appscale.model.PolicyStatus;
import io.netflix.titus.api.appscale.model.PolicyType;
import io.netflix.titus.api.appscale.model.Statistic;
import io.netflix.titus.api.appscale.model.StepAdjustment;
import io.netflix.titus.api.appscale.model.StepAdjustmentType;
import io.netflix.titus.api.appscale.model.StepScalingPolicyConfiguration;
import io.netflix.titus.api.json.ObjectMappers;
import io.netflix.titus.testkit.junit.category.IntegrationTest;
import org.assertj.core.api.Assertions;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

@Category(IntegrationTest.class)
public class CassAppScalePolicyStoreTest {
    private static Logger log = LoggerFactory.getLogger(CassAppScalePolicyStoreTest.class);

    private static final long STARTUP_TIMEOUT = 30_000L;
    private static final String CONFIGURATION_FILE_NAME = "relocated-cassandra.yaml";

    @Rule
    public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(
            new ClassPathCQLDataSet("tables.cql", "titus_integration_tests"),
            CONFIGURATION_FILE_NAME,
            STARTUP_TIMEOUT
    );

    private static String POLICY_1_ID = UUID.randomUUID().toString();
    private static String POLICY_2_ID = UUID.randomUUID().toString();
    private static String POLICY_3_ID = UUID.randomUUID().toString();


    private void loadTestData() throws Exception {
        Session session = cassandraCQLUnit.getSession();
        String insertStmt = "INSERT INTO app_scale_policy(ref_id, job_id, status, value) VALUES(?, ?, ?, ?);";
        PreparedStatement stmt = session.prepare(insertStmt);

        // record 1
        String jobId = "job-1";
        String serializedValue = ObjectMappers.appScalePolicyMapper().writeValueAsString(buildAutoScalingPolicy(jobId).getPolicyConfiguration());
        BoundStatement boundStatement = stmt.bind(UUID.fromString(POLICY_1_ID), jobId, PolicyStatus.Pending.name(), serializedValue);
        session.execute(boundStatement);

        // record 2
        String jobIdTwo = "job-2";
        String serializedValueTwo = ObjectMappers.appScalePolicyMapper().writeValueAsString(buildAutoScalingPolicy(jobIdTwo).getPolicyConfiguration());
        boundStatement = stmt.bind(UUID.fromString(POLICY_2_ID), jobIdTwo, PolicyStatus.Pending.name(), serializedValueTwo);
        session.execute(boundStatement);

        // record 3
        boundStatement = stmt.bind(UUID.fromString(POLICY_3_ID), jobId, PolicyStatus.Pending.name(), serializedValue);
        session.execute(boundStatement);

        // insert job-policy relationship
        insertStmt = "INSERT INTO app_scale_jobs(job_id, ref_id) VALUES(?, ?);";
        stmt = session.prepare(insertStmt);

        boundStatement = stmt.bind("job-1", UUID.fromString(POLICY_1_ID));
        session.execute(boundStatement);
        boundStatement = stmt.bind("job-1", UUID.fromString(POLICY_3_ID));
        session.execute(boundStatement);
        boundStatement = stmt.bind("job-2", UUID.fromString(POLICY_2_ID));
        session.execute(boundStatement);
    }

    @Test
    public void verifyStoreInit() throws Exception {
        Session session = cassandraCQLUnit.getSession();
        loadTestData();

        CassAppScalePolicyStore store = new CassAppScalePolicyStore(session, new DefaultRegistry());
        store.init().await();

        List<AutoScalingPolicy> allPolicies = store.retrievePolicies().toList().toBlocking().first();
        Assertions.assertThat(allPolicies.size()).isEqualTo(3);

        List<AutoScalingPolicy> jobOnePolicies = store.retrievePoliciesForJob("job-1").toList().toBlocking().first();
        Assertions.assertThat(jobOnePolicies.size()).isEqualTo(2);
        List<String> refIdList = jobOnePolicies.stream().map(as -> as.getRefId()).collect(Collectors.toList());
        Assertions.assertThat(refIdList).containsOnly(POLICY_1_ID, POLICY_3_ID);


        List<AutoScalingPolicy> jobTwoPolicies = store.retrievePoliciesForJob("job-2").toList().toBlocking().first();
        Assertions.assertThat(jobTwoPolicies.size()).isEqualTo(1);
        List<String> jobTwoRefIdList = jobTwoPolicies.stream().map(as -> as.getRefId()).collect(Collectors.toList());
        Assertions.assertThat(jobTwoRefIdList).isEqualTo(Arrays.asList(POLICY_2_ID));

        // verify metric lower/upper bounds
        List<StepAdjustment> stepAdjustments = jobTwoPolicies.stream()
                .flatMap(as -> as.getPolicyConfiguration().getStepScalingPolicyConfiguration().getSteps().stream())
                .collect(Collectors.toList());
        Assertions.assertThat(stepAdjustments.size()).isEqualTo(1);
        Assertions.assertThat(stepAdjustments.get(0).getMetricIntervalUpperBound()).isEqualTo(Optional.empty());
        Assertions.assertThat(stepAdjustments.get(0).getMetricIntervalLowerBound().get()).isEqualTo(Double.valueOf(0));
    }

    @Test
    public void checkStoreAndRetrieve() throws Exception {
        Session session = cassandraCQLUnit.getSession();
        CassAppScalePolicyStore store = new CassAppScalePolicyStore(session, new DefaultRegistry());

        String jobId = UUID.randomUUID().toString();
        Observable<String> respRefId = store.storePolicy(buildAutoScalingPolicy(jobId));
        String refId = respRefId.toBlocking().first();
        Assertions.assertThat(refId).isNotNull().isNotEmpty();

        Observable<AutoScalingPolicy> autoScalingPolicyObservable = store.retrievePolicyForRefId(refId);
        AutoScalingPolicy autoScalingPolicy = autoScalingPolicyObservable.toBlocking().first();
        Assertions.assertThat(autoScalingPolicy.getRefId()).isEqualTo(refId);
        Assertions.assertThat(autoScalingPolicy.getStatus()).isEqualTo(PolicyStatus.Pending);

        Observable<String> respRefIdTwo = store.storePolicy(buildAutoScalingPolicy(jobId));
        String refIdTwo = respRefIdTwo.toBlocking().first();
        Assertions.assertThat(refIdTwo).isNotNull().isNotEmpty();

        autoScalingPolicyObservable = store.retrievePoliciesForJob(jobId);
        List<AutoScalingPolicy> autoScalingPolicies = autoScalingPolicyObservable.toList().toBlocking().first();
        Assertions.assertThat(autoScalingPolicies.size()).isEqualTo(2);
        List<String> refIdList = autoScalingPolicies.stream().map(ap -> ap.getRefId()).collect(Collectors.toList());
        Assertions.assertThat(refIdList).isEqualTo(Arrays.asList(refId, refIdTwo));
        Assertions.assertThat(autoScalingPolicies.get(1).getStatus()).isEqualTo(PolicyStatus.Pending);


        autoScalingPolicyObservable = store.retrievePoliciesForJob("invalidJobId");
        List<AutoScalingPolicy> emptyPolicies = autoScalingPolicyObservable.toList().toBlocking().first();
        Assertions.assertThat(emptyPolicies.size()).isEqualTo(0);
    }

    @Test
    public void checkUpdates() throws Exception {
        Session session = cassandraCQLUnit.getSession();
        CassAppScalePolicyStore store = new CassAppScalePolicyStore(session, new DefaultRegistry());

        String jobId = UUID.randomUUID().toString();
        Observable<String> respRefId = store.storePolicy(buildAutoScalingPolicy(jobId));
        String refId = respRefId.toBlocking().first();
        Assertions.assertThat(refId).isNotNull().isNotEmpty();

        // update policyId
        String policyId = "PolicyARN";
        store.updatePolicyId(refId, policyId).await();
        String getPolicyIdQuery = "SELECT policy_id from app_scale_policy where ref_id = ?;";
        BoundStatement stmt = session.prepare(getPolicyIdQuery).bind(UUID.fromString(refId));
        ResultSet rs = session.execute(stmt);
        List<Row> rows = rs.all();
        Assertions.assertThat(rows.size()).isEqualTo(1);
        String policyIdStored = rows.get(0).getString(CassAppScalePolicyStore.COLUMN_POLICY_ID);
        Assertions.assertThat(policyIdStored).isEqualTo(policyId);

        // update alarmId
        String alarmId = "AlarmARM";
        store.updateAlarmId(refId, alarmId).await();
        String getAlarmIdQuery = "SELECT alarm_id from app_scale_policy where ref_id = ?;";
        stmt = session.prepare(getAlarmIdQuery).bind(UUID.fromString(refId));
        rs = session.execute(stmt);
        rows = rs.all();
        Assertions.assertThat(rows.size()).isEqualTo(1);
        String alarmIdStored = rows.get(0).getString(CassAppScalePolicyStore.COLUMN_ALARM_ID);
        Assertions.assertThat(alarmIdStored).isEqualTo(alarmId);

        // update policy status
        PolicyStatus status = PolicyStatus.Applied;
        store.updatePolicyStatus(refId, status).await();
        String getPolicyStatusQuery = "SELECT status from app_scale_policy where ref_id = ?;";
        stmt = session.prepare(getPolicyStatusQuery).bind(UUID.fromString(refId));
        rs = session.execute(stmt);
        rows = rs.all();
        Assertions.assertThat(rows.size()).isEqualTo(1);
        PolicyStatus updatedStatus = PolicyStatus.valueOf(rows.get(0).getString(CassAppScalePolicyStore.COLUMN_STATUS));
        Assertions.assertThat(updatedStatus).isEqualTo(status);

        String errorStatusMessage = "Got Trumped";
        store.updateStatusMessage(refId, errorStatusMessage).await();
        String getStatusMessageQuery = "SELECT status_message from app_scale_policy where ref_id = ?;";
        stmt = session.prepare(getStatusMessageQuery).bind(UUID.fromString(refId));
        rs = session.execute(stmt);
        rows = rs.all();
        Assertions.assertThat(rows.size()).isEqualTo(1);
        String statusMessage = rows.get(0).getString(CassAppScalePolicyStore.COLUMN_STATUS_MESSAGE);
        Assertions.assertThat(statusMessage).isEqualTo(errorStatusMessage);


        // read policy by refId
        Observable<AutoScalingPolicy> autoScalingPolicyObservable = store.retrievePolicyForRefId(refId);
        AutoScalingPolicy autoScalingPolicy = autoScalingPolicyObservable.toBlocking().first();
        Assertions.assertThat(autoScalingPolicy.getRefId()).isEqualTo(refId);
        Assertions.assertThat(autoScalingPolicy.getStatus()).isEqualTo(status);
        Assertions.assertThat(autoScalingPolicy.getAlarmId()).isEqualTo(alarmId);
        Assertions.assertThat(autoScalingPolicy.getPolicyId()).isEqualTo(policyId);

        // update alarm threshold
        AlarmConfiguration alarmConfiguration = autoScalingPolicy.getPolicyConfiguration().getAlarmConfiguration();
        double currentThreshold = alarmConfiguration.getThreshold();
        int thresholdIncrement = 10;
        AlarmConfiguration newAlarmConfiguration = AlarmConfiguration.newBuilder()
                .withStatistic(alarmConfiguration.getStatistic())
                .withName(alarmConfiguration.getName())
                .withMetricNamespace(alarmConfiguration.getMetricNamespace())
                .withMetricName(alarmConfiguration.getMetricName())
                .withComparisonOperator(alarmConfiguration.getComparisonOperator())
                .withAutoScalingGroupName(alarmConfiguration.getAutoScalingGroupName())
                .withThreshold(alarmConfiguration.getThreshold() + thresholdIncrement)
                .withPeriodSec(alarmConfiguration.getPeriodSec())
                .withEvaluationPeriods(alarmConfiguration.getEvaluationPeriods())
                .withActionsEnabled(alarmConfiguration.getActionsEnabled().get()).build();

        PolicyConfiguration newPolicyConfig = PolicyConfiguration.newBuilder()
                .withPolicyType(autoScalingPolicy.getPolicyConfiguration().getPolicyType())
                .withStepScalingPolicyConfiguration(autoScalingPolicy.getPolicyConfiguration().getStepScalingPolicyConfiguration())
                .withName(autoScalingPolicy.getPolicyConfiguration().getName())
                .withPolicyType(autoScalingPolicy.getPolicyConfiguration().getPolicyType())
                .withAlarmConfiguration(newAlarmConfiguration).build();

        AutoScalingPolicy newPolicy = AutoScalingPolicy.newBuilder()
                .withAutoScalingPolicy(autoScalingPolicy)
                .withPolicyConfiguration(newPolicyConfig).build();

        store.updatePolicyConfiguration(newPolicy).await();
        autoScalingPolicyObservable = store.retrievePolicyForRefId(refId);
        AutoScalingPolicy updatedPolicy = autoScalingPolicyObservable.toBlocking().first();
        Assertions.assertThat(updatedPolicy.getPolicyConfiguration().getAlarmConfiguration().getName())
                .isEqualTo(newAlarmConfiguration.getName());
        Assertions.assertThat(updatedPolicy.getPolicyConfiguration().getAlarmConfiguration().getThreshold())
                .isEqualTo(currentThreshold + thresholdIncrement);
    }


    @Test
    public void checkSerialization() throws Exception {
        AlarmConfiguration alarmConfiguration = AlarmConfiguration.newBuilder()
                .withActionsEnabled(true)
                .withAutoScalingGroupName("anyscale-amit-v000")
                .withComparisonOperator(ComparisonOperator.GreaterThanThreshold)
                .withEvaluationPeriods(1)
                .withPeriodSec(60)
                .withThreshold(2.5)
                .withMetricName("CPUUtilization")
                .withMetricNamespace("AWS/EC2")
                .withName("job-1")
                .withStatistic(Statistic.Average)
                .build();

        String serializedValue = ObjectMappers.appScalePolicyMapper().writeValueAsString(alarmConfiguration);
        alarmConfiguration = ObjectMappers.appScalePolicyMapper().readValue(serializedValue.getBytes(), AlarmConfiguration.class);
        Assertions.assertThat(alarmConfiguration.getName()).isEqualTo("job-1");
    }


    private AutoScalingPolicy buildAutoScalingPolicy(String jobId) {
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
}
