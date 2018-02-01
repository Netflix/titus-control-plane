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

package io.netflix.titus.ext.cassandra.tool.command;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.datastax.driver.core.Session;
import io.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.JobStatus;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import io.netflix.titus.api.jobmanager.store.JobStore;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.ext.cassandra.store.CassandraJobStore;
import io.netflix.titus.ext.cassandra.store.CassandraStoreConfiguration;
import io.netflix.titus.ext.cassandra.tool.Command;
import io.netflix.titus.ext.cassandra.tool.CommandContext;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import static io.netflix.titus.api.jobmanager.model.job.JobState.Accepted;

public class TestStoreLoadCommand implements Command {

    private static final Logger logger = LoggerFactory.getLogger(TestStoreLoadCommand.class);
    private static final int MAX_RETRIEVE_TASK_CONCURRENCY = 1_000;

    private static final CassandraStoreConfiguration CONFIGURATION = new CassandraStoreConfiguration() {
        @Override
        public boolean isFailOnInconsistentAgentData() {
            return true;
        }

        @Override
        public boolean isFailOnInconsistentLoadBalancerData() {
            return false;
        }

        @Override
        public int getConcurrencyLimit() {
            return MAX_RETRIEVE_TASK_CONCURRENCY;
        }
    };

    @Override
    public String getDescription() {
        return "Test the cassandra store implementation";
    }

    @Override
    public CommandType getCommandType() {
        return CommandType.TargetKeySpace;
    }

    @Override
    public Options getOptions() {
        Options options = new Options();
        options.addOption(Option.builder("j")
                .longOpt("jobs")
                .desc("The number of the jobs to create")
                .hasArg()
                .required()
                .build());
        options.addOption(Option.builder("ta")
                .longOpt("tasks")
                .desc("The number of the tasks to create per job")
                .hasArg()
                .required()
                .build());
        options.addOption(Option.builder("c")
                .longOpt("concurrency")
                .desc("The number of observables to run in parallel")
                .hasArg()
                .required()
                .build());
        options.addOption(Option.builder("i")
                .longOpt("iterations")
                .desc("The number of load iterations to run")
                .hasArg()
                .required()
                .build());
        return options;
    }

    @Override
    public void execute(CommandContext commandContext) {

        CommandLine commandLine = commandContext.getCommandLine();
        String keyspace = commandContext.getTargetKeySpace();
        Integer jobs = Integer.valueOf(commandLine.getOptionValue("jobs"));
        Integer tasks = Integer.valueOf(commandLine.getOptionValue("tasks"));
        Integer concurrency = Integer.valueOf(commandLine.getOptionValue("concurrency"));
        Integer iterations = Integer.valueOf(commandLine.getOptionValue("iterations"));
        Session session = commandContext.getTargetSession();

        boolean keyspaceExists = session.getCluster().getMetadata().getKeyspace(keyspace) != null;

        if (!keyspaceExists) {
            throw new IllegalStateException("Keyspace: " + keyspace + " does not exist. You must create it first.");
        }
        session.execute("USE " + keyspace);

        JobStore titusStore = new CassandraJobStore(CONFIGURATION, session);

        // Create jobs and tasks
        long jobStartTime = System.currentTimeMillis();
        List<Observable<Void>> createJobAndTasksObservables = new ArrayList<>();
        for (int i = 0; i < jobs; i++) {
            createJobAndTasksObservables.add(createJobAndTasksObservable(tasks, titusStore));
        }
        Observable.merge(createJobAndTasksObservables, concurrency).toBlocking().subscribe(
                none -> {
                },
                e -> logger.error("Error creating jobs: ", e),
                () -> {
                    logger.info("Created {} jobs with {} tasks in {}[ms]", jobs, tasks, System.currentTimeMillis() - jobStartTime);
                }
        );

        // try loading jobs and tasks for i iterations
        long loadTotalTime = 0L;
        for (int i = 0; i < iterations; i++) {
            long loadStartTime = System.currentTimeMillis();
            List<Pair<Job, List<Task>>> pairs = new ArrayList<>();
            titusStore.init().andThen(titusStore.retrieveJobs().toList().flatMap(retrievedJobs -> {
                List<Observable<Pair<Job, List<Task>>>> retrieveTasksObservables = new ArrayList<>();
                for (Job job : retrievedJobs) {
                    Observable<Pair<Job, List<Task>>> retrieveTasksObservable = titusStore.retrieveTasksForJob(job.getId())
                            .toList()
                            .map(taskList -> new Pair<>(job, taskList));
                    retrieveTasksObservables.add(retrieveTasksObservable);
                }
                return Observable.merge(retrieveTasksObservables, MAX_RETRIEVE_TASK_CONCURRENCY);
            })).map(p -> {
                pairs.add(p);
                return null;
            }).toBlocking().subscribe(
                    none -> {
                    },
                    e -> logger.error("Failed to load jobs from cassandra with error: ", e),
                    () -> {
                    }
            );
            long loadTime = System.currentTimeMillis() - loadStartTime;
            logger.info("Loaded {} jobs from cassandra in {}[ms]", pairs.size(), loadTime);
            loadTotalTime += loadTime;
        }

        logger.info("Average load time: {}[ms]", loadTotalTime / iterations);
    }

    private Observable<Void> createJobAndTasksObservable(int tasks, JobStore store) {
        Job<BatchJobExt> job = createJobObject();
        List<Task> taskList = new ArrayList<>();
        for (int i = 0; i < tasks; i++) {
            taskList.add(createTaskObject(job));
        }
        return store.storeJob(job).andThen(Observable.fromCallable(() -> {
            List<Observable<Void>> observables = new ArrayList<>();
            for (Task task : taskList) {
                observables.add(store.storeTask(task).toObservable());
            }
            return observables;
        })).flatMap(Observable::merge);
    }

    private Job<BatchJobExt> createJobObject() {
        String jobId = UUID.randomUUID().toString();
        JobDescriptor<BatchJobExt> jobDescriptor = JobDescriptor.<BatchJobExt>newBuilder()
                .withExtensions(new BatchJobExt(1, 1, null))
                .build();
        JobStatus status = new JobStatus(Accepted, "code", "message", System.currentTimeMillis());
        return new Job<>(jobId, jobDescriptor, status, new ArrayList<>());
    }

    private Task createTaskObject(Job<BatchJobExt> job) {
        String taskId = UUID.randomUUID().toString();
        return BatchJobTask.newBuilder()
                .withId(taskId)
                .withJobId(job.getId())
                .build();
    }
}
