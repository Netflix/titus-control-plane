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

package com.netflix.titus.ext.cassandra.store;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.QueryTrace;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.Version;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudget;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.model.job.migration.SystemDefaultMigrationPolicy;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.jobmanager.store.JobStore;
import com.netflix.titus.api.jobmanager.store.JobStoreException;
import com.netflix.titus.api.jobmanager.store.JobStoreFitAction;
import com.netflix.titus.api.json.ObjectMappers;
import com.netflix.titus.common.framework.fit.FitFramework;
import com.netflix.titus.common.framework.fit.FitInjection;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.guice.annotation.ProxyConfiguration;
import com.netflix.titus.common.util.tuple.Either;
import com.netflix.titus.common.util.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Emitter;
import rx.Observable;
import rx.exceptions.Exceptions;

import static com.netflix.titus.common.util.guice.ProxyType.Logging;
import static com.netflix.titus.common.util.guice.ProxyType.Spectator;
import static com.netflix.titus.ext.cassandra.store.StoreTransactionLoggers.transactionLogger;

@Singleton
@ProxyConfiguration(types = {Logging, Spectator})
public class CassandraJobStore implements JobStore {

    private static final Logger logger = LoggerFactory.getLogger(CassandraJobStore.class);

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
    private static final String RETRIEVE_ARCHIVED_TASKS_COUNT_STRING = "SELECT count(*) FROM archived_task_ids WHERE job_id = ?;";

    private final PreparedStatement retrieveActiveJobIdBucketsStatement;
    private final PreparedStatement retrieveActiveJobIdsStatement;
    private final PreparedStatement retrieveActiveJobStatement;
    private final PreparedStatement retrieveArchivedJobStatement;
    private final PreparedStatement retrieveActiveTaskIdsForJobStatement;
    private final PreparedStatement retrieveArchivedTaskIdsForJobStatement;
    private final PreparedStatement retrieveActiveTaskStatement;
    private final PreparedStatement retrieveArchivedTaskStatement;
    private final PreparedStatement retrieveArchivedTasksCountStatement;

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
    private static final String DELETE_ARCHIVED_TASK_ID_STRING = "DELETE FROM archived_task_ids WHERE job_id = ? and task_id = ?;";
    private static final String DELETE_ARCHIVED_TASK_STRING = "DELETE FROM archived_tasks WHERE task_id = ?;";

    private final PreparedStatement deleteActiveJobIdStatement;
    private final PreparedStatement deleteActiveJobStatement;
    private final PreparedStatement deleteActiveTaskIdStatement;
    private final PreparedStatement deleteActiveTaskStatement;
    private final PreparedStatement deletedArchivedTaskIdStatement;
    private final PreparedStatement deletedArchivedTaskStatement;

    private final TitusRuntime titusRuntime;
    private final Session session;
    private final ObjectMapper mapper;
    private final BalancedBucketManager<String> activeJobIdsBucketManager;
    private final CassandraStoreConfiguration configuration;
    private final Optional<FitInjection> fitDriverInjection;
    private final Optional<FitInjection> fitBadDataInjection;

    @Inject
    public CassandraJobStore(CassandraStoreConfiguration configuration,
                             Session session,
                             TitusRuntime titusRuntime) {
        this(configuration, session, titusRuntime, ObjectMappers.storeMapper(), INITIAL_BUCKET_COUNT, MAX_BUCKET_SIZE);
    }

    CassandraJobStore(CassandraStoreConfiguration configuration,
                      Session session,
                      TitusRuntime titusRuntime,
                      ObjectMapper mapper,
                      int initialBucketCount,
                      int maxBucketSize) {
        this.configuration = configuration;
        this.session = session;
        this.titusRuntime = titusRuntime;

        FitFramework fit = titusRuntime.getFitFramework();
        if (fit.isActive()) {
            FitInjection fitDriverInjection = fit.newFitInjectionBuilder("cassandraDriver")
                    .withDescription("Fail Cassandra driver requests")
                    .withExceptionType(DriverException.class)
                    .build();
            FitInjection fitBadDataInjection = fit.newFitInjectionBuilder("dataCorruption")
                    .withDescription("Corrupt data loaded from the database")
                    .build();
            fit.getRootComponent().getChild(V3JobOperations.COMPONENT)
                    .addInjection(fitDriverInjection)
                    .addInjection(fitBadDataInjection);

            this.fitDriverInjection = Optional.of(fitDriverInjection);
            this.fitBadDataInjection = Optional.of(fitBadDataInjection);
        } else {
            this.fitDriverInjection = Optional.empty();
            this.fitBadDataInjection = Optional.empty();
        }

        this.mapper = mapper;
        this.activeJobIdsBucketManager = new BalancedBucketManager<>(initialBucketCount, maxBucketSize, METRIC_NAME_ROOT, titusRuntime.getRegistry());

        retrieveActiveJobIdBucketsStatement = session.prepare(RETRIEVE_ACTIVE_JOB_ID_BUCKETS_STRING).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        retrieveActiveJobIdsStatement = session.prepare(RETRIEVE_ACTIVE_JOB_IDS_STRING).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        retrieveActiveJobStatement = session.prepare(RETRIEVE_ACTIVE_JOB_STRING).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        retrieveArchivedJobStatement = session.prepare(RETRIEVE_ARCHIVED_JOB_STRING).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        retrieveActiveTaskIdsForJobStatement = session.prepare(RETRIEVE_ACTIVE_TASK_IDS_FOR_JOB_STRING).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        retrieveArchivedTaskIdsForJobStatement = session.prepare(RETRIEVE_ARCHIVED_TASK_IDS_FOR_JOB_STRING).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        retrieveActiveTaskStatement = session.prepare(RETRIEVE_ACTIVE_TASK_STRING).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        retrieveArchivedTaskStatement = session.prepare(RETRIEVE_ARCHIVED_TASK_STRING).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        retrieveArchivedTasksCountStatement = session.prepare(RETRIEVE_ARCHIVED_TASKS_COUNT_STRING).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

        insertActiveJobStatement = session.prepare(INSERT_ACTIVE_JOB_STRING).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        insertActiveJobIdStatement = session.prepare(INSERT_ACTIVE_JOB_ID_STRING).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        insertArchivedJobStatement = session.prepare(INSERT_ARCHIVED_JOB_STRING).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        insertActiveTaskStatement = session.prepare(INSERT_ACTIVE_TASK_STRING).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        insertActiveTaskIdStatement = session.prepare(INSERT_ACTIVE_TASK_ID_STRING).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        insertArchivedTaskIdStatement = session.prepare(INSERT_ARCHIVED_TASK_ID_STRING).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        insertArchivedTaskStatement = session.prepare(INSERT_ARCHIVED_TASK_STRING).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

        deleteActiveJobIdStatement = session.prepare(DELETE_ACTIVE_JOB_ID_STRING).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        deleteActiveJobStatement = session.prepare(DELETE_ACTIVE_JOB_STRING).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        deleteActiveTaskIdStatement = session.prepare(DELETE_ACTIVE_TASK_ID_STRING).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        deleteActiveTaskStatement = session.prepare(DELETE_ACTIVE_TASK_STRING).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        deletedArchivedTaskIdStatement = session.prepare(DELETE_ARCHIVED_TASK_ID_STRING).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        deletedArchivedTaskStatement = session.prepare(DELETE_ARCHIVED_TASK_STRING).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
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

                                        if (fitBadDataInjection.isPresent()) {
                                            String effectiveJobId = fitBadDataInjection.get().afterImmediate(JobStoreFitAction.ErrorKind.LostJobIds.name(), jobId);
                                            if (effectiveJobId != null) {
                                                jobIds.add(effectiveJobId);
                                            }
                                            String phantomId = fitBadDataInjection.get().afterImmediate(JobStoreFitAction.ErrorKind.PhantomJobIds.name(), jobId);
                                            if (phantomId != null && !phantomId.equals(jobId)) {
                                                jobIds.add(phantomId);
                                            }
                                        } else {
                                            jobIds.add(jobId);
                                        }
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
    public Observable<Pair<List<Job<?>>, Integer>> retrieveJobs() {
        Observable result = Observable.fromCallable(() -> {
            List<String> jobIds = activeJobIdsBucketManager.getItems();
            return jobIds.stream().map(retrieveActiveJobStatement::bind).map(this::execute).collect(Collectors.toList());
        }).flatMap(observables -> Observable.merge(observables, getConcurrencyLimit()).flatMapIterable(resultSet -> {
            List<Row> allRows = resultSet.all();
            if (allRows.isEmpty()) {
                logger.debug("Job id with no record");
                return Collections.emptyList();
            }
            return allRows.stream()
                    .map(row -> row.getString(0))
                    .map(value -> {
                        String effectiveValue;
                        if (fitBadDataInjection.isPresent()) {
                            effectiveValue = fitBadDataInjection.get().afterImmediate(JobStoreFitAction.ErrorKind.CorruptedRawJobRecords.name(), value);
                        } else {
                            effectiveValue = value;
                        }

                        Job<?> job;
                        try {
                            job = deserializeJob(effectiveValue);
                        } catch (Exception e) {
                            logger.error("Cannot map serialized job data to Job class: {}", effectiveValue, e);
                            return Either.ofError(e);
                        }

                        if (job.getJobDescriptor().getDisruptionBudget() == null) {
                            titusRuntime.getCodeInvariants().inconsistent("jobWithNoDisruptionBudget: jobId=%s", job.getId());
                            job = JobFunctions.changeDisruptionBudget(job, DisruptionBudget.none());
                        }

                        // TODO Remove this code when there are no more jobs with missing migration data (caused by a bug in ServiceJobExt builder).
                        if (job.getJobDescriptor().getExtensions() instanceof ServiceJobExt) {
                            Job<ServiceJobExt> serviceJob = (Job<ServiceJobExt>) job;
                            ServiceJobExt ext = serviceJob.getJobDescriptor().getExtensions();
                            if (ext.getMigrationPolicy() == null) {
                                titusRuntime.getCodePointTracker().markReachable("Corrupted task migration record in Cassandra: " + job.getId());
                                ServiceJobExt fixedExt = ext.toBuilder().withMigrationPolicy(SystemDefaultMigrationPolicy.newBuilder().build()).build();
                                logger.warn("Service job with no migration policy defined. Setting system default: {}", job.getId());
                                job = serviceJob.toBuilder().withJobDescriptor(
                                        serviceJob.getJobDescriptor().toBuilder().withExtensions(fixedExt).build()
                                ).build();
                            }
                        }

                        if (!fitBadDataInjection.isPresent()) {
                            return Either.ofValue(job);
                        }

                        Job<?> effectiveJob = fitBadDataInjection.get().afterImmediate(JobStoreFitAction.ErrorKind.CorruptedJobRecords.name(), job);
                        return Either.ofValue(effectiveJob);
                    })
                    .collect(Collectors.toList());
        })).toList().map(everything -> {
            List<Job> goodJobs = (List<Job>) everything.stream().filter(Either::hasValue).map(Either::getValue).collect(Collectors.toList());
            int errors = everything.size() - goodJobs.size();
            return Pair.of(goodJobs, errors);
        });

        return result;
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
            return deserializeJob(value);
        }));
    }

    @Override
    public Completable storeJob(Job job) {
        return Observable
                .fromCallable((Callable<Statement>) () -> {
                    String jobId = job.getId();
                    checkIfJobAlreadyExists(jobId);

                    String jobJsonString = writeJobToString(job);

                    int bucket = activeJobIdsBucketManager.getNextBucket();
                    activeJobIdsBucketManager.addItem(bucket, jobId);
                    Statement jobStatement = insertActiveJobStatement.bind(jobId, jobJsonString);
                    Statement jobIdStatement = insertActiveJobIdStatement.bind(bucket, jobId);

                    BatchStatement batchStatement = new BatchStatement();
                    batchStatement.add(jobStatement);
                    batchStatement.add(jobIdStatement);

                    transactionLogger().logBeforeCreate(insertActiveJobStatement, "storeJob", job);

                    return batchStatement;
                })
                .flatMap(statement -> execute(statement)
                        .doOnNext(rs -> transactionLogger().logAfterCreate(insertActiveJobStatement, "storeJob", job))
                        .doOnError(throwable -> activeJobIdsBucketManager.deleteItem(job.getId()))
                )
                .toCompletable();
    }

    private String writeJobToString(Job job) {
        return ObjectMappers.writeValueAsString(mapper, job);
    }

    @Override
    public Completable updateJob(Job job) {
        return Observable
                .fromCallable((Callable<Statement>) () -> {
                    String jobId = job.getId();
                    checkIfJobIsActive(jobId);
                    String jobJsonString = writeJobToString(job);

                    transactionLogger().logBeforeUpdate(insertActiveJobStatement, "updateJob", job);
                    return insertActiveJobStatement.bind(jobId, jobJsonString);
                })
                .flatMap(statement ->
                        execute(statement).doOnNext(rs -> transactionLogger().logAfterUpdate(insertActiveJobStatement, "updateJob", job))
                )
                .toCompletable();
    }

    @Override
    public Completable deleteJob(Job job) {
        return Observable.fromCallable(() -> {
            String jobId = job.getId();
            checkIfJobIsActive(jobId);
            return jobId;
        }).flatMap(jobId -> retrieveTasksForJob(jobId).flatMap(tasksAndErrors -> {
            List<Task> tasks = tasksAndErrors.getLeft();

            int errors = tasksAndErrors.getRight();
            if (errors > 0) {
                logger.warn("Some tasks records could not be loaded during the job delete operation. Ignoring them: {}", errors);
            }
            List<Task> fixedTasks = checkTaskConsistency(tasks);

            List<Completable> completables = fixedTasks.stream().map(this::deleteTask).collect(Collectors.toList());
            return Completable.merge(Observable.from(completables), getConcurrencyLimit()).toObservable();
        })).toList().flatMap(ignored -> {
            BatchStatement statement = getArchiveJobBatchStatement(job);

            transactionLogger().logBeforeDelete(deleteActiveJobStatement, "deleteJob", job);

            return execute(statement).doOnNext(rs -> transactionLogger().logAfterDelete(deleteActiveJobStatement, "deleteJob", job));
        }).flatMap(ignored -> {
            activeJobIdsBucketManager.deleteItem(job.getId());
            return Observable.empty();
        }).toCompletable();
    }

    private List<Task> checkTaskConsistency(List<Task> tasks) {
        List<Task> checkedTasks = new ArrayList<>();

        for (Task task : tasks) {
            if (task.getStatus().getState() == TaskState.Finished) {
                checkedTasks.add(task);
            } else {
                titusRuntime.getCodeInvariants().inconsistent("Archiving task that is not in Finished state: task={}", task);
                Task fixed = JobFunctions.fixArchivedTaskStatus(task, titusRuntime.getClock());
                checkedTasks.add(fixed);
            }
        }
        return checkedTasks;
    }

    @Override
    public Observable<Pair<List<Task>, Integer>> retrieveTasksForJob(String jobId) {
        return Observable.fromCallable(() -> {
            checkIfJobIsActive(jobId);
            return retrieveActiveTaskIdsForJobStatement.bind(jobId).setFetchSize(Integer.MAX_VALUE);
        }).flatMap(retrieveActiveTaskIdsForJob -> execute(retrieveActiveTaskIdsForJob).flatMap(taskIdsResultSet -> {
            List<String> taskIds = taskIdsResultSet.all().stream()
                    .map(row -> row.getString(0))
                    .flatMap(taskId -> {
                        if (fitBadDataInjection.isPresent()) {
                            List<String> effectiveTaskIds = new ArrayList<>();
                            String effectiveTaskId = fitBadDataInjection.get().afterImmediate(JobStoreFitAction.ErrorKind.LostTaskIds.name(), taskId);
                            if (effectiveTaskId != null) {
                                effectiveTaskIds.add(effectiveTaskId);
                            }
                            String phantomId = fitBadDataInjection.get().afterImmediate(JobStoreFitAction.ErrorKind.PhantomTaskIds.name(), taskId);
                            if (phantomId != null && !phantomId.equals(taskId)) {
                                effectiveTaskIds.add(phantomId);
                            }
                            return effectiveTaskIds.stream();
                        }
                        return Stream.of(taskId);
                    })
                    .collect(Collectors.toList());

            List<Observable<ResultSet>> observables = taskIds.stream().map(retrieveActiveTaskStatement::bind).map(this::execute).collect(Collectors.toList());

            return Observable.merge(observables, getConcurrencyLimit()).flatMapIterable(tasksResultSet -> {
                List<Either<Task, Throwable>> tasks = new ArrayList<>();
                for (Row row : tasksResultSet.all()) {
                    String value = row.getString(0);

                    String effectiveValue;
                    if (fitBadDataInjection.isPresent()) {
                        effectiveValue = fitBadDataInjection.get().afterImmediate(JobStoreFitAction.ErrorKind.CorruptedRawTaskRecords.name(), value);
                    } else {
                        effectiveValue = value;
                    }

                    Task task;
                    try {
                        task = deserializeTask(effectiveValue);

                        if (!fitBadDataInjection.isPresent()) {
                            tasks.add(Either.ofValue(task));
                        } else {
                            Task effectiveTask = fitBadDataInjection.get().afterImmediate(JobStoreFitAction.ErrorKind.CorruptedTaskRecords.name(), task);
                            effectiveTask = fitBadDataInjection.get().afterImmediate(JobStoreFitAction.ErrorKind.DuplicatedEni.name(), effectiveTask);
                            effectiveTask = fitBadDataInjection.get().afterImmediate(JobStoreFitAction.ErrorKind.CorruptedTaskPlacementData.name(), effectiveTask);
                            tasks.add(Either.ofValue(effectiveTask));
                        }

                        transactionLogger().logAfterRead(retrieveActiveTaskStatement, "retrieveTasksForJob", task);
                    } catch (Exception e) {
                        logger.error("Cannot map serialized task data to Task class: {}", effectiveValue, e);
                        tasks.add(Either.ofError(e));
                    }
                }
                return tasks;
            });
        })).toList().map(taskErrorPairs -> {
            List<Task> tasks = taskErrorPairs.stream().filter(Either::hasValue).map(Either::getValue).collect(Collectors.toList());
            int errors = (int) taskErrorPairs.stream().filter(Either::hasError).count();
            return Pair.of(tasks, errors);
        });
    }

    @Override
    public Observable<Task> retrieveTask(String taskId) {
        return Observable
                .fromCallable((Callable<Statement>) () -> {
                    transactionLogger().logBeforeRead(retrieveActiveTaskStatement, "retrieveTask", taskId);
                    return retrieveActiveTaskStatement.bind(taskId);
                })
                .flatMap(statement -> execute(statement).flatMap(resultSet -> {
                    Row row = resultSet.one();
                    if (row != null) {
                        String value = row.getString(0);
                        Task task = deserializeTask(value);

                        transactionLogger().logAfterRead(retrieveActiveTaskStatement, "retrieveTask", task);

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

            transactionLogger().logBeforeCreate(insertActiveTaskStatement, "storeTask", task);

            return batchStatement;
        }).flatMap(statement ->
                execute(statement).doOnNext(rs -> transactionLogger().logAfterCreate(insertActiveTaskStatement, "storeTask", task))
        ).toCompletable();
    }

    @Override
    public Completable updateTask(Task task) {
        return Observable.fromCallable((Callable<Statement>) () -> {
            String jobId = task.getJobId();
            String taskId = task.getId();
            checkIfJobIsActive(jobId);
            String taskJsonString = ObjectMappers.writeValueAsString(mapper, task);

            transactionLogger().logBeforeUpdate(insertActiveTaskStatement, "updateTask", task);

            return insertActiveTaskStatement.bind(taskId, taskJsonString);
        }).flatMap(statement -> {
                    transactionLogger().logAfterUpdate(insertActiveTaskStatement, "updateTask", task);
                    return execute(statement);
                }
        ).toCompletable();
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

    /**
     * Moving task between jobs requires the following Cassandra updates:
     * <ul>
     * <li>Update the active_jobs table with the new jobFrom record</li>
     * <li>Update the active_jobs table with the new jobTo record</li>
     * <li>Update task record in the active_tasks table (to include the new job id)</li>
     * <li>Remove a record from the active_task_ids table for the jobFrom/taskId pair</li>
     * <li>Add a new record in the active_task_ids for the jobTo/taskId pair</li>
     * </ul>
     */
    @Override
    public Completable moveTask(Job jobFrom, Job jobTo, Task taskAfter) {
        return Observable.fromCallable((Callable<Statement>) () -> {
            checkIfJobIsActive(jobFrom.getId());
            checkIfJobIsActive(jobTo.getId());

            String taskJsonString = ObjectMappers.writeValueAsString(mapper, taskAfter);
            transactionLogger().logBeforeUpdate(insertActiveTaskStatement, "moveTask", taskJsonString);

            BatchStatement batchStatement = new BatchStatement();
            batchStatement.add(insertActiveJobStatement.bind(jobFrom.getId(), ObjectMappers.writeValueAsString(mapper, jobFrom)));
            batchStatement.add(insertActiveJobStatement.bind(jobTo.getId(), ObjectMappers.writeValueAsString(mapper, jobTo)));
            batchStatement.add(insertActiveTaskStatement.bind(taskAfter.getId(), taskJsonString));
            batchStatement.add(deleteActiveTaskIdStatement.bind(jobFrom.getId(), taskAfter.getId()));
            batchStatement.add(insertActiveTaskIdStatement.bind(jobTo.getId(), taskAfter.getId()));

            return batchStatement;
        }).flatMap(this::execute).toCompletable().doOnCompleted(() -> transactionLogger().logAfterUpdate(insertActiveTaskStatement, "moveTask", taskAfter));
    }

    @Override
    public Completable deleteTask(Task task) {
        return Observable.fromCallable((Callable<Statement>) () -> {
            String jobId = task.getJobId();
            checkIfJobIsActive(jobId);

            BatchStatement archiveTaskBatchStatement = getArchiveTaskBatchStatement(task);

            transactionLogger().logBeforeDelete(archiveTaskBatchStatement, "deleteTask", task);

            return archiveTaskBatchStatement;
        }).flatMap(statement -> {
                    transactionLogger().logAfterDelete(statement, "deleteTask", task);
                    return execute(statement);
                }
        ).toCompletable();
    }

    /**
     * This method reads data from the archive table, and if not found checks the active table for its existence.
     * The latter is needed as sometimes a job may not be correctly archived, and we do not have a reconciliation process
     * that would fix it.
     */
    @Override
    public Observable<Job<?>> retrieveArchivedJob(String jobId) {
        Observable<Job> action = retrieveEntityById(jobId, Job.class, retrieveArchivedJobStatement)
                .switchIfEmpty(retrieveEntityById(jobId, Job.class, retrieveActiveJobStatement)
                        .filter(job -> job.getStatus().getState() == JobState.Finished)
                )
                .switchIfEmpty(Observable.error(JobStoreException.jobDoesNotExist(jobId)));
        return (Observable) action;
    }

    /**
     * This method reads data from the archive table, and if not found checks the active table for its existence.
     * The latter is needed as sometimes a job may not be correctly archived, and we do not have a reconciliation process
     * that would fix it.
     */
    @Override
    public Observable<Task> retrieveArchivedTasksForJob(String jobId) {
        return retrieveTasksForJob(jobId, retrieveArchivedTaskIdsForJobStatement, retrieveArchivedTaskStatement)
                .switchIfEmpty(retrieveTasksForJob(jobId, retrieveActiveTaskIdsForJobStatement, retrieveActiveTaskStatement)
                        .filter(task -> task.getStatus().getState() == TaskState.Finished)
                );
    }

    private Observable<Task> retrieveTasksForJob(String jobId, PreparedStatement taskIdStatement, PreparedStatement taskStatement) {
        return Observable.fromCallable(() -> taskIdStatement.bind(jobId).setFetchSize(Integer.MAX_VALUE))
                .flatMap(retrieveActiveTaskIdsForJob ->
                        execute(retrieveActiveTaskIdsForJob).flatMap(taskIdsResultSet -> {
                            List<String> taskIds = taskIdsResultSet.all().stream().map(row -> row.getString(0)).collect(Collectors.toList());
                            if (taskIds.isEmpty()) {
                                return Observable.empty();
                            }
                            List<Observable<ResultSet>> observables = taskIds.stream()
                                    .map(taskStatement::bind)
                                    .map(this::execute)
                                    .collect(Collectors.toList());
                            return Observable.merge(observables, getConcurrencyLimit()).flatMapIterable(tasksResultSet -> tasksResultSet.all().stream()
                                    .map(row -> row.getString(0))
                                    .map(this::deserializeTask)
                                    .collect(Collectors.toList()));
                        }));
    }

    /**
     * This method reads data from the archive table, and if not found checks the active table for its existence.
     * The latter is needed as sometimes a task may not be correctly archived, and we do not have a reconciliation process
     * that would fix it.
     * <p>
     * This method should be only used to get state of a task belonging to a finished job. It would return a persisted
     * state for a task belonging to an active job as well, as a side effect for the workaround implemented here.
     * A caller should not piggyback on this behavior, as it may change at any point in time.
     */
    @Override
    public Observable<Task> retrieveArchivedTask(String taskId) {
        return retrieveEntityById(taskId, Task.class, retrieveArchivedTaskStatement)
                .switchIfEmpty(retrieveEntityById(taskId, Task.class, retrieveActiveTaskStatement)
                        .filter(task -> task.getStatus().getState() == TaskState.Finished)
                )
                .switchIfEmpty(Observable.error(JobStoreException.taskDoesNotExist(taskId)));
    }

    /**
     * This method counts the number of archived tasks for a given job id.
     */
    @Override
    public Observable<Long> retrieveArchivedTaskCountForJob(String jobId) {
        return retrieveEntityById(jobId, Long.class, retrieveArchivedTasksCountStatement);
    }

    /**
     * This method deletes an archived task.
     */
    @Override
    public Completable deleteArchivedTask(String jobId, String taskId) {
        return Observable.fromCallable((Callable<Statement>) () -> {
            BatchStatement deleteArchivedTaskBatchStatement = getDeleteArchivedTaskBatchStatement(jobId, taskId);

            transactionLogger().logBeforeDelete(deleteArchivedTaskBatchStatement, "deleteArchivedTask", taskId);

            return deleteArchivedTaskBatchStatement;
        }).flatMap(statement -> {
                    transactionLogger().logAfterDelete(statement, "deleteArchivedTask", taskId);
                    return execute(statement);
                }
        ).toCompletable();
    }

    private <T> Observable<T> retrieveEntityById(String id, Class<T> type, PreparedStatement preparedStatement) {
        return Observable.fromCallable((Callable<Statement>) () -> preparedStatement.bind(id))
                .flatMap(this::execute)
                .flatMap(resultSet -> {
                    Row row = resultSet.one();
                    if (row == null) {
                        return Observable.empty();
                    }
                    try {
                        if (type == Long.class) {
                            return Observable.just(type.cast(row.getLong(0)));
                        }
                        String value = row.getString(0);
                        if (type.isAssignableFrom(Job.class)) {
                            return Observable.just(type.cast(deserializeJob(value)));
                        }
                        if (type.isAssignableFrom(Task.class)) {
                            return Observable.just(type.cast(deserializeTask(value)));
                        }
                        return Observable.just(ObjectMappers.readValue(mapper, value, type));
                    } catch (Exception e) {
                        return Observable.error(e);
                    }
                });
    }

    private Job<?> ensureHasVersion(Job<?> job) {
        if (job.getVersion() == null || job.getVersion().getTimestamp() < 0) {
            if (job.getStatus() != null) {
                Version newVersion = Version.newBuilder().withTimestamp(job.getStatus().getTimestamp()).build();
                return job.toBuilder().withVersion(newVersion).build();
            }
        }
        return job;
    }

    private Task ensureHasVersion(Task task) {
        if (task.getVersion() == null || task.getVersion().getTimestamp() < 0) {
            if (task.getStatus() != null) {
                Version newVersion = Version.newBuilder().withTimestamp(task.getStatus().getTimestamp()).build();
                return task.toBuilder().withVersion(newVersion).build();
            }
        }
        return task;
    }

    private Job<?> deserializeJob(String value) {
        Job job = ObjectMappers.readValue(mapper, value, Job.class);
        job = ensureHasVersion(job);
        return job;
    }

    private Task deserializeTask(String value) {
        Task task = ObjectMappers.readValue(mapper, value, Task.class);

        // Task attributes field check
        if (task.getAttributes() == null) {
            task = task.toBuilder().withAttributes(Collections.emptyMap()).build();
        }

        task = ensureHasVersion(task);

        return task;
    }

    private boolean isJobActive(String jobId) {
        return activeJobIdsBucketManager.itemExists(jobId);
    }

    private BatchStatement getArchiveJobBatchStatement(Job job) {
        String jobId = job.getId();
        int bucket = activeJobIdsBucketManager.getItemBucket(jobId);
        String jobJsonString = writeJobToString(job);

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

    private BatchStatement getDeleteArchivedTaskBatchStatement(String jobId, String taskId) {
        Statement deleteArchivedTaskIdStatement = deletedArchivedTaskIdStatement.bind(jobId, taskId);
        Statement deleteArchivedTaskStatement = deletedArchivedTaskStatement.bind(taskId);

        BatchStatement batchStatement = new BatchStatement();
        batchStatement.add(deleteArchivedTaskIdStatement);
        batchStatement.add(deleteArchivedTaskStatement);

        return batchStatement;
    }

    private Observable<ResultSet> execute(Statement statement) {
        return Observable.<ResultSet>create(
                emitter -> {
                    boolean tracingEnabled = configuration.isTracingEnabled();
                    Statement modifiedStatement = tracingEnabled ? statement.enableTracing() : statement;
                    ListenableFuture<ResultSet> resultSetFuture = fitDriverInjection
                            .map(injection -> injection.aroundListenableFuture(
                                    "executeAsync", () -> session.executeAsync(modifiedStatement))
                            )
                            .orElseGet(() -> session.executeAsync(modifiedStatement));

                    Futures.addCallback(resultSetFuture, new FutureCallback<ResultSet>() {
                        @Override
                        public void onSuccess(@Nullable ResultSet result) {
                            if (result != null && tracingEnabled) {
                                QueryTrace queryTrace = result.getExecutionInfo().getQueryTrace();
                                if (queryTrace != null) {
                                    logger.info("Executed statement with traceId: {}", queryTrace.getTraceId());
                                }
                            }
                            emitter.onNext(result);
                            emitter.onCompleted();
                        }

                        @Override
                        public void onFailure(@Nonnull Throwable e) {
                            emitter.onError(JobStoreException.cassandraDriverError(e));
                        }
                    }, MoreExecutors.directExecutor());
                    emitter.setCancellation(() -> resultSetFuture.cancel(true));
                },
                Emitter.BackpressureMode.NONE
        ).doOnError(e -> logger.error("Cassandra operation error: {}", e.getMessage()));
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
