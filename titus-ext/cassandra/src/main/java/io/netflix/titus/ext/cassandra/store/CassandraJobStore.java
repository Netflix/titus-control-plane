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
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.netflix.spectator.api.Registry;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.store.JobStore;
import io.netflix.titus.api.jobmanager.store.JobStoreException;
import io.netflix.titus.api.json.ObjectMappers;
import io.netflix.titus.common.util.guice.annotation.ProxyConfiguration;
import rx.Completable;
import rx.Emitter;
import rx.Observable;
import rx.exceptions.Exceptions;

import static io.netflix.titus.common.util.guice.ProxyType.Logging;
import static io.netflix.titus.common.util.guice.ProxyType.Spectator;

@Singleton
@ProxyConfiguration(types = {Logging, Spectator})
public class CassandraJobStore implements JobStore {
    private static final int INITIAL_BUCKET_COUNT = 100;
    private static final int MAX_BUCKET_SIZE = 2_000;
    private static final String METRIC_NAME_ROOT = "titusMaster.jobManager.cassandra";

    // SELECT Queries
    private static final String RETRIEVE_ACTIVE_JOB_ID_BUCKETS_STRING = "SELECT distinct bucket FROM active_job_ids";
    private static final String RETRIEVE_ACTIVE_JOB_IDS_STRING = "SELECT job_id FROM active_job_ids WHERE bucket = ?;";
    private static final String RETRIEVE_ACTIVE_JOB_STRING = "SELECT value FROM active_jobs WHERE job_id = ?;";
    private static final String RETRIEVE_ARCHIVED_JOB_STRING = "SELECT value FROM archived_jobs WHERE job_id = ?;";
    private static final String RETRIEVE_ACTIVE_TASK_IDS_FOR_JOB_STRING = "SELECT task_id FROM active_task_ids WHERE job_id = ?;";
    private static final String RETRIEVE_ARCHIVED_TASK_IDS_FOR_JOB_STRING = "SELECT task_id FROM archived_task_ids WHERE job_id = ?;";
    private static final String RETRIEVE_ACTIVE_TASK_STRING = "SELECT value FROM active_tasks WHERE task_id = ?;";
    private static final String RETRIEVE_ARCHIVED_TASK_STRING = "SELECT value FROM archived_tasks WHERE task_id = ?;";

    private final PreparedStatement retrieveActiveJobIdBucketsStatement;
    private final PreparedStatement retrieveActiveJobIdsStatement;
    private final PreparedStatement retrieveActiveJobStatement;
    private final PreparedStatement retrieveArchivedJobStatement;
    private final PreparedStatement retrieveActiveTaskIdsForJobStatement;
    private final PreparedStatement retrieveArchivedTaskIdsForJobStatement;
    private final PreparedStatement retrieveActiveTaskStatement;
    private final PreparedStatement retrieveArchivedTaskStatement;

    // INSERT Queries
    private static final String INSERT_ACTIVE_JOB_ID_STRING = "INSERT INTO active_job_ids (bucket, job_id) VALUES (?, ?);";
    private static final String INSERT_ACTIVE_JOB_STRING = "INSERT INTO active_jobs (job_id, value) VALUES (?, ?);";
    private static final String INSERT_ARCHIVED_JOB_STRING = "INSERT INTO archived_jobs (job_id, value) VALUES (?, ?);";
    private static final String INSERT_ACTIVE_TASK_ID_STRING = "INSERT INTO active_task_ids (job_id, task_id) VALUES (?, ?);";
    private static final String INSERT_ACTIVE_TASK_STRING = "INSERT INTO active_tasks (task_id, value) VALUES (?, ?);";
    private static final String INSERT_ARCHIVED_TASK_ID_STRING = "INSERT INTO archived_task_ids (job_id, task_id) VALUES (?, ?);";
    private static final String INSERT_ARCHIVED_TASK_STRING = "INSERT INTO archived_tasks (task_id, value) VALUES (?, ?);";

    private final PreparedStatement insertActiveJobStatement;
    private final PreparedStatement insertActiveJobIdStatement;
    private final PreparedStatement insertArchivedJobStatement;
    private final PreparedStatement insertActiveTaskStatement;
    private final PreparedStatement insertActiveTaskIdStatement;
    private final PreparedStatement insertArchivedTaskIdStatement;
    private final PreparedStatement insertArchivedTaskStatement;

    // DELETE Queries
    private static final String DELETE_ACTIVE_JOB_ID_STRING = "DELETE FROM active_job_ids WHERE bucket = ? and job_id = ?";
    private static final String DELETE_ACTIVE_JOB_STRING = "DELETE FROM active_jobs WHERE job_id = ?;";
    private static final String DELETE_ACTIVE_TASK_ID_STRING = "DELETE FROM active_task_ids WHERE job_id = ? and task_id = ?";
    private static final String DELETE_ACTIVE_TASK_STRING = "DELETE FROM active_tasks WHERE task_id = ?;";

    private final PreparedStatement deleteActiveJobIdStatement;
    private final PreparedStatement deleteActiveJobStatement;
    private final PreparedStatement deleteActiveTaskIdStatement;
    private final PreparedStatement deleteActiveTaskStatement;

    private final Session session;
    private final ObjectMapper mapper;
    private final BalancedBucketManager<String> activeJobIdsBucketManager;
    private final CassandraStoreConfiguration configuration;

    @Inject
    public CassandraJobStore(CassandraStoreConfiguration configuration, Session session, Registry registry) {
        this(configuration, session, registry, ObjectMappers.storeMapper(), INITIAL_BUCKET_COUNT, MAX_BUCKET_SIZE);
    }

    CassandraJobStore(CassandraStoreConfiguration configuration,
                      Session session,
                      Registry registry,
                      ObjectMapper mapper,
                      int initialBucketCount,
                      int maxBucketSize) {
        this.configuration = configuration;
        this.session = session;
        this.mapper = mapper;
        this.activeJobIdsBucketManager = new BalancedBucketManager<>(initialBucketCount, maxBucketSize, METRIC_NAME_ROOT, registry);

        retrieveActiveJobIdBucketsStatement = session.prepare(RETRIEVE_ACTIVE_JOB_ID_BUCKETS_STRING);
        retrieveActiveJobIdsStatement = session.prepare(RETRIEVE_ACTIVE_JOB_IDS_STRING);
        retrieveActiveJobStatement = session.prepare(RETRIEVE_ACTIVE_JOB_STRING);
        retrieveArchivedJobStatement = session.prepare(RETRIEVE_ARCHIVED_JOB_STRING);
        retrieveActiveTaskIdsForJobStatement = session.prepare(RETRIEVE_ACTIVE_TASK_IDS_FOR_JOB_STRING);
        retrieveArchivedTaskIdsForJobStatement = session.prepare(RETRIEVE_ARCHIVED_TASK_IDS_FOR_JOB_STRING);
        retrieveActiveTaskStatement = session.prepare(RETRIEVE_ACTIVE_TASK_STRING);
        retrieveArchivedTaskStatement = session.prepare(RETRIEVE_ARCHIVED_TASK_STRING);

        insertActiveJobStatement = session.prepare(INSERT_ACTIVE_JOB_STRING);
        insertActiveJobIdStatement = session.prepare(INSERT_ACTIVE_JOB_ID_STRING);
        insertArchivedJobStatement = session.prepare(INSERT_ARCHIVED_JOB_STRING);
        insertActiveTaskStatement = session.prepare(INSERT_ACTIVE_TASK_STRING);
        insertActiveTaskIdStatement = session.prepare(INSERT_ACTIVE_TASK_ID_STRING);
        insertArchivedTaskIdStatement = session.prepare(INSERT_ARCHIVED_TASK_ID_STRING);
        insertArchivedTaskStatement = session.prepare(INSERT_ARCHIVED_TASK_STRING);

        deleteActiveJobIdStatement = session.prepare(DELETE_ACTIVE_JOB_ID_STRING);
        deleteActiveJobStatement = session.prepare(DELETE_ACTIVE_JOB_STRING);
        deleteActiveTaskIdStatement = session.prepare(DELETE_ACTIVE_TASK_ID_STRING);
        deleteActiveTaskStatement = session.prepare(DELETE_ACTIVE_TASK_STRING);
    }

    @Override
    public Completable init() {
        return Observable.fromCallable(() -> retrieveActiveJobIdBucketsStatement.bind().setFetchSize(Integer.MAX_VALUE))
                .flatMap(statement -> execute(statement).flatMap(resultSet -> {
                    List<Completable> completables = new ArrayList<>();
                    for (Row row : resultSet.all()) {
                        int bucket = row.getInt(0);
                        Statement retrieveJobIdsStatement = retrieveActiveJobIdsStatement.bind(bucket).setFetchSize(Integer.MAX_VALUE);
                        Completable completable = execute(retrieveJobIdsStatement)
                                .flatMap(jobIdsResultSet -> {
                                    List<String> jobIds = new ArrayList<>();
                                    for (Row jobIdRow : jobIdsResultSet.all()) {
                                        String jobId = jobIdRow.getString(0);
                                        jobIds.add(jobId);
                                    }
                                    activeJobIdsBucketManager.addItems(bucket, jobIds);
                                    return Observable.empty();
                                }).toCompletable();
                        completables.add(completable);
                    }
                    return Completable.merge(Observable.from(completables), getConcurrencyLimit()).toObservable();
                })).toCompletable();
    }

    @Override
    public Observable<Job<?>> retrieveJobs() {
        return Observable.fromCallable(() -> {
            List<String> jobIds = activeJobIdsBucketManager.getItems();
            return jobIds.stream().map(retrieveActiveJobStatement::bind).map(this::execute).collect(Collectors.toList());
        }).flatMap(observables -> Observable.merge(observables, getConcurrencyLimit()).flatMapIterable(resultSet -> resultSet.all().stream()
                .map(row -> row.getString(0))
                .map(value -> (Job<?>) ObjectMappers.readValue(mapper, value, Job.class))
                .collect(Collectors.toList())));
    }

    @Override
    public Observable<Job<?>> retrieveJob(String jobId) {
        return Observable.fromCallable((Callable<Statement>) () -> {
            checkIfJobIsActive(jobId);
            return retrieveActiveJobStatement.bind(jobId);
        }).flatMap(statement -> execute(statement).map(resultSet -> {
            Row row = resultSet.one();
            if (row == null) {
                throw JobStoreException.jobDoesNotExist(jobId);
            }
            String value = row.getString(0);
            return (Job<?>) ObjectMappers.readValue(mapper, value, Job.class);
        }));
    }

    @Override
    public Completable storeJob(Job job) {
        return Observable.fromCallable((Callable<Statement>) () -> {
            String jobId = job.getId();
            checkIfJobAlreadyExists(jobId);
            String jobJsonString = ObjectMappers.writeValueAsString(mapper, job);
            int bucket = activeJobIdsBucketManager.getNextBucket();
            activeJobIdsBucketManager.addItem(bucket, jobId);
            Statement jobStatement = insertActiveJobStatement.bind(jobId, jobJsonString);
            Statement jobIdStatement = insertActiveJobIdStatement.bind(bucket, jobId);

            BatchStatement batchStatement = new BatchStatement();
            batchStatement.add(jobStatement);
            batchStatement.add(jobIdStatement);
            return batchStatement;
        }).flatMap(statement -> execute(statement).doOnError(throwable -> activeJobIdsBucketManager.deleteItem(job.getId())))
                .toCompletable();
    }

    @Override
    public Completable updateJob(Job job) {
        return Observable.fromCallable((Callable<Statement>) () -> {
            String jobId = job.getId();
            checkIfJobIsActive(jobId);
            String jobJsonString = ObjectMappers.writeValueAsString(mapper, job);
            return insertActiveJobStatement.bind(jobId, jobJsonString);
        }).flatMap(this::execute).toCompletable();
    }

    @Override
    public Completable deleteJob(Job job) {
        return Observable.fromCallable(() -> {
            String jobId = job.getId();
            checkIfJobIsActive(jobId);
            return jobId;
        }).flatMap(jobId -> retrieveTasksForJob(jobId).toList().flatMap(tasks -> {
            List<Completable> completables = tasks.stream().map(this::deleteTask).collect(Collectors.toList());
            return Completable.merge(Observable.from(completables), getConcurrencyLimit()).toObservable();
        })).toList().flatMap(ignored -> {
            BatchStatement statement = getArchiveJobBatchStatement(job);
            return execute(statement);
        }).flatMap(ignored -> {
            activeJobIdsBucketManager.deleteItem(job.getId());
            return Observable.empty();
        }).toCompletable();
    }

    @Override
    public Observable<Task> retrieveTasksForJob(String jobId) {
        return Observable.fromCallable(() -> {
            checkIfJobIsActive(jobId);
            return retrieveActiveTaskIdsForJobStatement.bind(jobId).setFetchSize(Integer.MAX_VALUE);
        }).flatMap(retrieveActiveTaskIdsForJob -> execute(retrieveActiveTaskIdsForJob).flatMap(taskIdsResultSet -> {
            List<String> taskIds = taskIdsResultSet.all().stream().map(row -> row.getString(0)).collect(Collectors.toList());
            List<Observable<ResultSet>> observables = taskIds.stream().map(retrieveActiveTaskStatement::bind).map(this::execute).collect(Collectors.toList());

            return Observable.merge(observables, getConcurrencyLimit()).flatMapIterable(tasksResultSet -> {
                List<Task> tasks = new ArrayList<>();
                for (Row row : tasksResultSet.all()) {
                    String value = row.getString(0);
                    Task task = ObjectMappers.readValue(mapper, value, Task.class);
                    tasks.add(task);
                }
                return tasks;
            });
        }));
    }

    @Override
    public Observable<Task> retrieveTask(String taskId) {
        return Observable.fromCallable((Callable<Statement>) () -> retrieveActiveTaskStatement.bind(taskId))
                .flatMap(statement -> execute(statement).flatMap(resultSet -> {
                    Row row = resultSet.one();
                    if (row != null) {
                        String value = row.getString(0);
                        Task task = ObjectMappers.readValue(mapper, value, Task.class);
                        return Observable.just(task);
                    } else {
                        return Observable.error(JobStoreException.taskDoesNotExist(taskId));
                    }
                }));
    }

    @Override
    public Completable storeTask(Task task) {
        return Observable.fromCallable((Callable<Statement>) () -> {
            String jobId = task.getJobId();
            String taskId = task.getId();
            checkIfJobIsActive(jobId);
            String taskJsonString = ObjectMappers.writeValueAsString(mapper, task);
            Statement taskStatement = insertActiveTaskStatement.bind(taskId, taskJsonString);
            Statement taskIdStatement = insertActiveTaskIdStatement.bind(jobId, taskId);

            BatchStatement batchStatement = new BatchStatement();
            batchStatement.add(taskStatement);
            batchStatement.add(taskIdStatement);

            return batchStatement;
        }).flatMap(this::execute).toCompletable();
    }

    @Override
    public Completable updateTask(Task task) {
        return Observable.fromCallable((Callable<Statement>) () -> {
            String jobId = task.getJobId();
            String taskId = task.getId();
            checkIfJobIsActive(jobId);
            String taskJsonString = ObjectMappers.writeValueAsString(mapper, task);
            return insertActiveTaskStatement.bind(taskId, taskJsonString);
        }).flatMap(this::execute).toCompletable();
    }

    @Override
    public Completable replaceTask(Task oldTask, Task newTask) {
        return Observable.fromCallable((Callable<Statement>) () -> {
            String jobId = newTask.getJobId();
            checkIfJobIsActive(jobId);
            String taskId = newTask.getId();
            String taskJsonString = ObjectMappers.writeValueAsString(mapper, newTask);

            BatchStatement batchStatement = getArchiveTaskBatchStatement(oldTask);

            Statement insertTaskStatement = insertActiveTaskStatement.bind(taskId, taskJsonString);
            Statement insertTaskIdStatement = insertActiveTaskIdStatement.bind(jobId, taskId);

            batchStatement.add(insertTaskStatement);
            batchStatement.add(insertTaskIdStatement);

            return batchStatement;
        }).flatMap(this::execute).toCompletable();
    }

    @Override
    public Completable deleteTask(Task task) {
        return Observable.fromCallable((Callable<Statement>) () -> {
            String jobId = task.getJobId();
            checkIfJobIsActive(jobId);
            return getArchiveTaskBatchStatement(task);
        }).flatMap(this::execute).toCompletable();
    }

    @Override
    public Observable<Job<?>> retrieveArchivedJob(String jobId) {
        return Observable.fromCallable((Callable<Statement>) () -> retrieveArchivedJobStatement.bind(jobId)).flatMap(statement -> execute(statement)
                .map(resultSet -> {
                    Row row = resultSet.one();
                    if (row == null) {
                        throw JobStoreException.jobDoesNotExist(jobId);
                    }
                    String value = row.getString(0);
                    return (Job<?>) ObjectMappers.readValue(mapper, value, Job.class);
                }));
    }

    @Override
    public Observable<Task> retrieveArchivedTasksForJob(String jobId) {
        return Observable.fromCallable(() -> retrieveArchivedTaskIdsForJobStatement.bind(jobId).setFetchSize(Integer.MAX_VALUE))
                .flatMap(retrieveActiveTaskIdsForJob -> execute(retrieveActiveTaskIdsForJob).flatMap(taskIdsResultSet -> {
                    List<String> taskIds = taskIdsResultSet.all().stream().map(row -> row.getString(0)).collect(Collectors.toList());
                    List<Observable<ResultSet>> observables = taskIds.stream().map(retrieveArchivedTaskStatement::bind).map(this::execute).collect(Collectors.toList());
                    return Observable.merge(observables, getConcurrencyLimit()).flatMapIterable(tasksResultSet -> tasksResultSet.all().stream()
                            .map(row -> row.getString(0))
                            .map(value -> ObjectMappers.readValue(mapper, value, Task.class))
                            .collect(Collectors.toList()));
                }));
    }

    @Override
    public Observable<Task> retrieveArchivedTask(String taskId) {
        return Observable.fromCallable((Callable<Statement>) () -> retrieveArchivedTaskStatement.bind(taskId))
                .flatMap(statement -> execute(statement).flatMap(resultSet -> {
                    Row row = resultSet.one();
                    if (row != null) {
                        String value = row.getString(0);
                        Task task = ObjectMappers.readValue(mapper, value, Task.class);
                        return Observable.just(task);
                    } else {
                        return Observable.error(JobStoreException.taskDoesNotExist(taskId));
                    }
                }));
    }

    private boolean isJobActive(String jobId) {
        return activeJobIdsBucketManager.itemExists(jobId);
    }

    private BatchStatement getArchiveJobBatchStatement(Job job) {
        String jobId = job.getId();
        int bucket = activeJobIdsBucketManager.getItemBucket(jobId);
        String jobJsonString = ObjectMappers.writeValueAsString(mapper, job);

        Statement deleteJobStatement = deleteActiveJobStatement.bind(jobId);
        Statement deleteJobIdStatement = deleteActiveJobIdStatement.bind(bucket, jobId);
        Statement insertJobStatement = insertArchivedJobStatement.bind(jobId, jobJsonString);

        BatchStatement statement = new BatchStatement();
        statement.add(deleteJobStatement);
        statement.add(deleteJobIdStatement);
        statement.add(insertJobStatement);

        return statement;
    }

    private BatchStatement getArchiveTaskBatchStatement(Task task) {
        String jobId = task.getJobId();
        String taskId = task.getId();
        String taskJsonString = ObjectMappers.writeValueAsString(mapper, task);

        Statement deleteTaskStatement = deleteActiveTaskStatement.bind(taskId);
        Statement deleteTaskIdStatement = deleteActiveTaskIdStatement.bind(jobId, taskId);
        Statement insertTaskStatement = insertArchivedTaskStatement.bind(taskId, taskJsonString);
        Statement insertTaskIdStatement = insertArchivedTaskIdStatement.bind(jobId, taskId);

        BatchStatement batchStatement = new BatchStatement();
        batchStatement.add(deleteTaskStatement);
        batchStatement.add(deleteTaskIdStatement);
        batchStatement.add(insertTaskStatement);
        batchStatement.add(insertTaskIdStatement);

        return batchStatement;
    }

    private Observable<ResultSet> execute(Statement statement) {
        return Observable.create(emitter -> {
            ResultSetFuture resultSetFuture = session.executeAsync(statement);
            Futures.addCallback(resultSetFuture, new FutureCallback<ResultSet>() {
                @Override
                public void onSuccess(@Nullable ResultSet result) {
                    emitter.onNext(result);
                    emitter.onCompleted();
                }

                @Override
                public void onFailure(@Nonnull Throwable e) {
                    emitter.onError(JobStoreException.cassandraDriverError(e));
                }
            });
            emitter.setCancellation(() -> resultSetFuture.cancel(true));
        }, Emitter.BackpressureMode.NONE);
    }

    private int getConcurrencyLimit() {
        return Math.max(2, configuration.getConcurrencyLimit());
    }

    private void checkIfJobIsActive(String jobId) {
        if (!isJobActive(jobId)) {
            throw Exceptions.propagate(JobStoreException.jobMustBeActive(jobId));
        }
    }

    private void checkIfJobAlreadyExists(String jobId) {
        if (isJobActive(jobId)) {
            throw Exceptions.propagate(JobStoreException.jobAlreadyExists(jobId));
        }
    }
}
