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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import io.netflix.titus.api.appscale.model.AutoScalingPolicy;
import io.netflix.titus.api.appscale.model.PolicyConfiguration;
import io.netflix.titus.api.appscale.model.PolicyStatus;
import io.netflix.titus.api.appscale.store.AppScalePolicyStore;
import io.netflix.titus.api.json.ObjectMappers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;

@Singleton
public class CassAppScalePolicyStore implements AppScalePolicyStore {
    private static Logger log = LoggerFactory.getLogger(CassAppScalePolicyStore.class);
    public static final String COLUMN_REF_ID = "ref_id";
    public static final String COLUMN_JOB_ID = "job_id";
    public static final String COLUMN_VALUE = "value";
    public static final String COLUMN_POLICY_ID = "policy_id";
    public static final String COLUMN_ALARM_ID = "alarm_id";
    public static final String COLUMN_STATUS = "status";
    public static final String COLUMN_STATUS_MESSAGE = "status_message";


    public static final String METRIC_APP_SCALE_STORE_CREATE_POLICY = "titus.appscale.store.create.policy";
    public static final String METRIC_APP_SCALE_STORE_UPDATE_POLICY = "titus.appscale.store.update.policy";

    private final CassStoreHelper storeHelper;
    private final PreparedStatement insertNewPolicyStmt;
    private final PreparedStatement insertJobIdWithPolicyRefStmt;
    private final PreparedStatement getPolicyByRefIdStmt;
    private final PreparedStatement updatePolicyConfigStmt;
    private final PreparedStatement updatePolicyIdStmt;
    private final PreparedStatement updateAlarmIdStmt;
    private final PreparedStatement updatePolicyStatusStmt;
    private final PreparedStatement updateStatusMessageStmt;
    private final PreparedStatement getAllJobIdsStmt;
    private final Registry registry;
    private final Counter createPolicyCounter;
    private final Counter updatePolicyCounter;

    private Session session;

    private static String GET_ALL_JOB_IDS = "SELECT * FROM app_scale_jobs;";
    private static String GET_POLICY_BY_ID = "SELECT * FROM app_scale_policy where ref_id = ?;";
    private static final String INSERT_NEW_POLICY = "INSERT INTO app_scale_policy(ref_id, job_id, status, value) VALUES (?, ?, ?, ?);";
    private static final String INSERT_JOB_ID_WITH_POLICY_REF_ID = "INSERT INTO app_scale_jobs(job_id, ref_id) VALUES (?, ?);";
    private static final String UPDATE_POLICY_CONFIG = "UPDATE app_scale_policy set value = ? where ref_id = ?;";
    private static final String UPDATE_POLICY_ALARM_ID = "UPDATE app_scale_policy set alarm_id = ? where ref_id = ?;";
    private static final String UPDATE_POLICY_STATUS = "UPDATE app_scale_policy set status = ? where ref_id = ?;";
    private static final String UPDATE_POLICY_POLICY_ID = "UPDATE app_scale_policy set policy_id = ? where ref_id = ?;";
    private static final String UPDATE_STATUS_MESSAGE = "UPDATE app_scale_policy set status_message = ? where ref_id = ?;";


    private volatile Map<String, AutoScalingPolicy> policies;
    private volatile Map<String, List<String>> policyRefIdsForJob;


    @Inject
    public CassAppScalePolicyStore(Session session, Registry registry) {

        this.session = session;
        this.registry = registry;
        this.insertNewPolicyStmt = this.session.prepare(INSERT_NEW_POLICY);
        this.insertJobIdWithPolicyRefStmt = this.session.prepare(INSERT_JOB_ID_WITH_POLICY_REF_ID);
        this.getPolicyByRefIdStmt = this.session.prepare(GET_POLICY_BY_ID);
        this.updateAlarmIdStmt = this.session.prepare(UPDATE_POLICY_ALARM_ID);
        this.updatePolicyConfigStmt = this.session.prepare(UPDATE_POLICY_CONFIG);
        this.updatePolicyIdStmt = this.session.prepare(UPDATE_POLICY_POLICY_ID);
        this.updatePolicyStatusStmt = this.session.prepare(UPDATE_POLICY_STATUS);
        this.updateStatusMessageStmt = this.session.prepare(UPDATE_STATUS_MESSAGE);
        this.getAllJobIdsStmt = this.session.prepare(GET_ALL_JOB_IDS);

        this.storeHelper = new CassStoreHelper(session);
        this.policies = new ConcurrentHashMap<>();
        this.policyRefIdsForJob = new ConcurrentHashMap<>();

        createPolicyCounter = registry.counter(METRIC_APP_SCALE_STORE_CREATE_POLICY);
        updatePolicyCounter = registry.counter(METRIC_APP_SCALE_STORE_UPDATE_POLICY);
    }

    @Override
    public Completable init() {
        return storeHelper.execute(getAllJobIdsStmt.bind().setFetchSize(Integer.MAX_VALUE))
                .flatMap(rs -> Observable.from(rs.all()))
                .map(row -> {
                    String refId = row.getUUID(COLUMN_REF_ID).toString();
                    String jobId = row.getString(COLUMN_JOB_ID);
                    updatePolicyRefIdsForJobMap(jobId, refId);
                    return refId;
                })
                .map(refId -> getPolicyByRefIdStmt.bind().setUUID(0, UUID.fromString(refId)).setFetchSize(Integer.MAX_VALUE))
                .flatMap(stmt -> storeHelper.execute(stmt))
                .flatMap(rs -> Observable.from(rs.all()))
                .map(row -> buildAutoScalingPolicyFromRow(row))
                .map(autoScalingPolicy -> policies.putIfAbsent(autoScalingPolicy.getRefId(), autoScalingPolicy))
                .toCompletable();

    }

    @Override
    public Observable<AutoScalingPolicy> retrievePolicies(boolean includeArchived) {
        List<AutoScalingPolicy> validPolicies = policies.values().stream()
                .filter(autoScalingPolicy ->
                        autoScalingPolicy.getStatus() == PolicyStatus.Pending ||
                                autoScalingPolicy.getStatus() == PolicyStatus.Deleting ||
                                autoScalingPolicy.getStatus() == PolicyStatus.Error ||
                                autoScalingPolicy.getStatus() == PolicyStatus.Applied ||
                                (includeArchived && autoScalingPolicy.getStatus() == PolicyStatus.Deleted))
                .collect(Collectors.toList());
        log.info("Retrieving {} valid policies", validPolicies.size());
        return Observable.from(validPolicies);
    }

    @Override
    public Observable<String> storePolicy(AutoScalingPolicy autoScalingPolicy) {
        return Observable.fromCallable(() -> ObjectMappers.writeValueAsString(ObjectMappers.appScalePolicyMapper(), autoScalingPolicy.getPolicyConfiguration())
        ).flatMap(policyStr -> {
            UUID refId = UUID.randomUUID();
            BoundStatement statement = insertNewPolicyStmt.bind(refId, autoScalingPolicy.getJobId(), PolicyStatus.Pending.name(), policyStr);
            return storeHelper.execute(statement).map(emptyResultSet -> refId);
        }).flatMap(refId -> {
            BoundStatement statement = insertJobIdWithPolicyRefStmt.bind(autoScalingPolicy.getJobId(), refId);
            return storeHelper.execute(statement).map(emptyResultSet -> refId);
        }).map(refId -> {
            AutoScalingPolicy updatedPolicy = AutoScalingPolicy.newBuilder()
                    .withAutoScalingPolicy(autoScalingPolicy)
                    .withStatus(PolicyStatus.Pending)
                    .withRefId(refId.toString()).build();
            policies.putIfAbsent(refId.toString(), updatedPolicy);
            createPolicyCounter.increment();
            return refId.toString();
        }).map(refId -> {
            updatePolicyRefIdsForJobMap(autoScalingPolicy.getJobId(), refId);
            return refId.toString();
        });
    }

    @Override
    public Completable updatePolicyConfiguration(AutoScalingPolicy autoScalingPolicy) {
        return Observable.fromCallable(() ->
                ObjectMappers.writeValueAsString(ObjectMappers.appScalePolicyMapper(), autoScalingPolicy.getPolicyConfiguration())
        ).flatMap(policyStr -> {
            BoundStatement statement = updatePolicyConfigStmt.bind(policyStr, UUID.fromString(autoScalingPolicy.getRefId()));
            return storeHelper.execute(statement);
        }).map(storeUpdated -> {
            policies.put(autoScalingPolicy.getRefId(), autoScalingPolicy);
            updatePolicyCounter.increment();
            return storeUpdated;
        }).toCompletable();
    }


    @Override
    public Completable updatePolicyId(String policyRefId, String policyId) {
        return Observable.fromCallable(() -> {
            BoundStatement statement = updatePolicyIdStmt.bind(policyId, UUID.fromString(policyRefId));
            return storeHelper.execute(statement);
        }).flatMap(storeUpdated -> {
            AutoScalingPolicy autoScalingPolicy = policies.get(policyRefId);
            AutoScalingPolicy updatedPolicy = AutoScalingPolicy.newBuilder().withAutoScalingPolicy(autoScalingPolicy).withPolicyId(policyId).build();
            policies.put(policyRefId, updatedPolicy);
            updatePolicyCounter.increment();
            return storeUpdated;
        }).toCompletable();
    }


    @Override
    public Completable updateAlarmId(String policyRefId, String alarmId) {
        return Observable.fromCallable(() -> {
            BoundStatement statement = updateAlarmIdStmt.bind(alarmId, UUID.fromString(policyRefId));
            return storeHelper.execute(statement);
        }).flatMap(storeUpdated -> {
            AutoScalingPolicy autoScalingPolicy = policies.get(policyRefId);
            AutoScalingPolicy updatedPolicy = AutoScalingPolicy.newBuilder().withAutoScalingPolicy(autoScalingPolicy)
                    .withAlarmId(alarmId).build();
            policies.put(policyRefId, updatedPolicy);
            updatePolicyCounter.increment();
            return storeUpdated;
        }).toCompletable();
    }


    @Override
    public Completable updatePolicyStatus(String policyRefId, PolicyStatus policyStatus) {
        return Observable.fromCallable(() -> {
            BoundStatement statement = updatePolicyStatusStmt.bind(policyStatus.name(), UUID.fromString(policyRefId));
            return storeHelper.execute(statement);
        }).flatMap(storeUpdated -> {
            AutoScalingPolicy autoScalingPolicy = policies.get(policyRefId);
            AutoScalingPolicy updatedPolicy = AutoScalingPolicy.newBuilder().withAutoScalingPolicy(autoScalingPolicy)
                    .withStatus(policyStatus).build();
            policies.put(policyRefId, updatedPolicy);
            updatePolicyCounter.increment();
            return storeUpdated;
        }).toCompletable();
    }

    @Override
    public Completable updateStatusMessage(String policyRefId, String statusMessage) {
        return Observable.fromCallable(() -> {
            BoundStatement statement = updateStatusMessageStmt.bind(statusMessage, UUID.fromString(policyRefId));
            return storeHelper.execute(statement);
        }).flatMap(storeUpdated -> {
            AutoScalingPolicy autoScalingPolicy = policies.get(policyRefId);
            AutoScalingPolicy updatedPolicy = AutoScalingPolicy.newBuilder().withAutoScalingPolicy(autoScalingPolicy)
                    .withStatusMessage(statusMessage).build();
            policies.put(policyRefId, updatedPolicy);
            updatePolicyCounter.increment();
            return storeUpdated;
        }).toCompletable();
    }


    @Override
    public Observable<AutoScalingPolicy> retrievePoliciesForJob(String jobId) {
        return Observable.fromCallable(() -> {
            if (!policyRefIdsForJob.containsKey(jobId)) {
                return new ArrayList<String>();
            }
            return policyRefIdsForJob.get(jobId);
        }).flatMap(refIdList -> Observable.from(refIdList))
                .map(refId -> policies.get(refId))
                .filter(autoScalingPolicy ->
                        autoScalingPolicy != null &&
                                autoScalingPolicy.getStatus() != null &&
                                (autoScalingPolicy.getStatus() == PolicyStatus.Pending ||
                                        autoScalingPolicy.getStatus() == PolicyStatus.Error ||
                                        autoScalingPolicy.getStatus() == PolicyStatus.Deleting ||
                                        autoScalingPolicy.getStatus() == PolicyStatus.Applied));
    }


    @Override
    public Observable<AutoScalingPolicy> retrievePolicyForRefId(String policyRefId) {
        if (policies.containsKey(policyRefId)) {
            return Observable.just(policies.get(policyRefId));
        }
        return Observable.empty();
    }

    @Override
    public Completable removePolicy(String policyRefId) {
        return updatePolicyStatus(policyRefId, PolicyStatus.Deleting);
    }


    private AutoScalingPolicy buildAutoScalingPolicyFromRow(Row row) {
        String refId = row.getUUID(COLUMN_REF_ID).toString();
        String jobId = row.getString(COLUMN_JOB_ID);
        String policyConfigurationStr = row.getString(COLUMN_VALUE);
        String policyId = row.getString(COLUMN_POLICY_ID);
        String alarmId = row.getString(COLUMN_ALARM_ID);
        String status = row.getString(COLUMN_STATUS);
        String statusMessage = row.getString(COLUMN_STATUS_MESSAGE);

        PolicyConfiguration policyConfiguration = ObjectMappers.readValue(ObjectMappers.appScalePolicyMapper(), policyConfigurationStr, PolicyConfiguration.class);
        return AutoScalingPolicy.newBuilder()
                .withRefId(refId)
                .withJobId(jobId)
                .withPolicyConfiguration(policyConfiguration)
                .withAlarmId(alarmId)
                .withPolicyId(policyId)
                .withStatus(PolicyStatus.valueOf(status))
                .withStatusMessage(statusMessage)
                .build();
    }

    private void updatePolicyRefIdsForJobMap(String jobId, String refId) {
        List<String> existingValue = policyRefIdsForJob.putIfAbsent(jobId, new ArrayList<>(Arrays.asList(refId)));
        if (existingValue != null) {
            policyRefIdsForJob.computeIfPresent(jobId, (jid, currentList) -> {
                currentList.add(refId);
                return currentList;
            });
        }
    }
}
