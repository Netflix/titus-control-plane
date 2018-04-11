package com.netflix.titus.ext.cassandra.tool.command;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.json.ObjectMappers;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.ext.cassandra.tool.CassandraUtils;
import com.netflix.titus.ext.cassandra.tool.Command;
import com.netflix.titus.ext.cassandra.tool.CommandContext;
import org.apache.commons.cli.Options;

import static com.netflix.titus.ext.cassandra.tool.CassandraSchemas.ACTIVE_JOBS_TABLE;
import static com.netflix.titus.ext.cassandra.tool.CassandraSchemas.ACTIVE_JOB_IDS_TABLE;
import static com.netflix.titus.ext.cassandra.tool.CassandraSchemas.ACTIVE_TASKS_TABLE;
import static com.netflix.titus.ext.cassandra.tool.CassandraSchemas.ACTIVE_TASK_IDS_TABLE;

public class JobReconcilerCommand implements Command {

    @Override
    public String getDescription() {
        return "Report inconsistencies in V3 job data";
    }

    @Override
    public CommandType getCommandType() {
        return CommandType.TargetKeySpace;
    }

    @Override
    public Options getOptions() {
        return new Options();
    }

    @Override
    public void execute(CommandContext context) {
        Reconciler reconciler = new Reconciler(context);
        reconciler.loadAllTables();
        reconciler.checkDataConsistency();
        reconciler.printReport();
    }

    private class Reconciler {

        private final CommandContext context;
        private final Map<String, Integer> tableSizes = new HashMap<>();
        private int violationCounter;
        private final List<Pair<String, String>> violations = new ArrayList<>();

        private Set<String> jobIds;
        private List<Job<?>> jobs;
        private Map<String, String> taskIdToJobIdMap;
        private List<Task> tasks;

        private Reconciler(CommandContext context) {
            this.context = context;
        }

        private void loadAllTables() {
            jobIds = loadJobIds();
            System.out.println("Loaded jobIds: " + jobIds.size());

            jobs = loadJobs();
            System.out.println("Loaded job: " + jobs.size());

            taskIdToJobIdMap = loadTaskIdToJobIdMapping();
            System.out.println("Loaded taskIds: " + taskIdToJobIdMap.size());

            tasks = loadTasks();
            System.out.println("Loaded tasks: " + tasks.size());
        }

        private void checkDataConsistency() {
            checkJobIdToJobMapping();
            checkTaskToJobIdMapping();
            checkTaskReachabilityFromJob();
        }

        private Set<String> loadJobIds() {
            List<Pair<Object, Object>> bucketToJobIdList = CassandraUtils.readTwoColumnTable(context.getTargetSession(), ACTIVE_JOB_IDS_TABLE).toList().toBlocking().first();
            tableSizes.put(ACTIVE_JOB_IDS_TABLE, bucketToJobIdList.size());

            // Check that each job is associated with unique bucket id.
            Map<String, Integer> jobIdToBucketMap = new HashMap<>();
            bucketToJobIdList.forEach(pair -> {
                int bucketId = (Integer) pair.getLeft();
                String jobId = (String) pair.getRight();
                Integer previousBucketId = jobIdToBucketMap.get(jobId);
                if (previousBucketId != null) {
                    recordViolation("multipleJobIdToBucketMappings", String.format("Job %s is mapped to buckets %s and %s", jobId, previousBucketId, bucketId), 1);
                } else {
                    jobIdToBucketMap.put(jobId, bucketId);
                }
            });

            return new HashSet<>(jobIdToBucketMap.keySet());
        }

        private List<Job<?>> loadJobs() {
            List<Pair<Object, Object>> jobIdToJobList = CassandraUtils.readTwoColumnTable(context.getTargetSession(), ACTIVE_JOBS_TABLE).toList().toBlocking().first();
            tableSizes.put(ACTIVE_JOBS_TABLE, jobIdToJobList.size());

            return jobIdToJobList.stream()
                    .map(pair -> {
                        String jobId = (String) pair.getLeft();
                        String body = (String) pair.getRight();
                        try {
                            return (Job<?>) ObjectMappers.storeMapper().readValue(body, Job.class);
                        } catch (Exception e) {
                            recordViolation("badJobRecord", String.format("Job %s cannot be mapped to Job object: %s", jobId, e.getMessage()), 1);
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        }

        private Map<String, String> loadTaskIdToJobIdMapping() {
            List<Pair<Object, Object>> jobIdToTaskIdList = CassandraUtils.readTwoColumnTable(context.getTargetSession(), ACTIVE_TASK_IDS_TABLE).toList().toBlocking().first();
            tableSizes.put(ACTIVE_TASK_IDS_TABLE, jobIdToTaskIdList.size());

            // Check that each task id is associated with exactly one job id.
            Map<String, String> taskIdToJobIdMapping = new HashMap<>();
            jobIdToTaskIdList.forEach(pair -> {
                String jobId = (String) pair.getLeft();
                String taskId = (String) pair.getRight();
                String previousJobId = taskIdToJobIdMapping.get(taskId);
                if (previousJobId != null) {
                    recordViolation("multipleTaskIdToJobIdMappings", String.format("Task %s is mapped to job %s and %s", taskId, previousJobId, jobId), 1);
                } else {
                    taskIdToJobIdMapping.put(taskId, jobId);
                }
            });

            return taskIdToJobIdMapping;
        }

        private List<Task> loadTasks() {
            List<Pair<Object, Object>> taskIdToTaskList = CassandraUtils.readTwoColumnTable(context.getTargetSession(), ACTIVE_TASKS_TABLE).toList().toBlocking().first();
            tableSizes.put(ACTIVE_TASKS_TABLE, taskIdToTaskList.size());

            return taskIdToTaskList.stream()
                    .map(pair -> {
                        String taskId = (String) pair.getLeft();
                        String body = (String) pair.getRight();
                        try {
                            return ObjectMappers.storeMapper().readValue(body, Task.class);
                        } catch (Exception e) {
                            recordViolation("badTaskRecord", String.format("Task %s cannot be mapped to Task object: %s", taskId, e.getMessage()), 1);
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        }

        private void checkJobIdToJobMapping() {
            Set<String> jobRecordIds = jobs.stream().map(Job::getId).collect(Collectors.toSet());

            Set<String> unusedJobIds = CollectionsExt.copyAndRemove(jobIds, jobRecordIds);
            if (!unusedJobIds.isEmpty()) {
                recordViolation("unusedJobIds", String.format("Found jobIds not associated with any job record: %s", unusedJobIds), unusedJobIds.size());
            }

            Set<String> jobsWithNoBucketMapping = CollectionsExt.copyAndRemove(jobRecordIds, jobIds);
            if (!jobsWithNoBucketMapping.isEmpty()) {
                recordViolation("jobsWithNoBucketMapping", String.format("Found job records not associated with any jobId bucket: %s", jobsWithNoBucketMapping), jobsWithNoBucketMapping.size());
            }
        }

        private void checkTaskToJobIdMapping() {
            Set<String> taskRecordIds = tasks.stream().map(Task::getId).collect(Collectors.toSet());

            Set<String> unusedTaskIds = CollectionsExt.copyAndRemove(taskIdToJobIdMap.keySet(), taskRecordIds);
            if (!unusedTaskIds.isEmpty()) {
                recordViolation("unusedTaskIds", String.format("Found taskIds not associated with any task record: %s", unusedTaskIds), unusedTaskIds.size());
            }

            Set<String> tasksWithNoJobIdMapping = CollectionsExt.copyAndRemove(taskRecordIds, taskIdToJobIdMap.keySet());
            if (!tasksWithNoJobIdMapping.isEmpty()) {
                recordViolation("tasksWithNoJobIdMapping", String.format("Found task records not associated with any jobId: %s", tasksWithNoJobIdMapping), tasksWithNoJobIdMapping.size());
            }
        }

        private void checkTaskReachabilityFromJob() {
            Map<String, List<Task>> jobIdToTaskMapping = new HashMap<>();
            tasks.forEach(task -> jobIdToTaskMapping.computeIfAbsent(task.getJobId(), jid -> new ArrayList<>()).add(task));

            Set<String> unknownJobs = CollectionsExt.copyAndRemove(jobIdToTaskMapping.keySet(), jobIds);
            if (!unknownJobs.isEmpty()) {
                Set<String> badTaskIds = tasks.stream()
                        .filter(t -> !jobIdToTaskMapping.containsKey(t.getJobId()) || unknownJobs.contains(t.getJobId()))
                        .map(Task::getId)
                        .collect(Collectors.toSet());
                recordViolation(
                        "tasksNotAssociatedWithLoadableJob",
                        String.format("Found task records not associated with any loadable job: invalidJobIds=%s, taskIds=%s", unknownJobs, badTaskIds),
                        badTaskIds.size()
                );
            }
        }

        private void printReport() {
            System.out.println("################################################################################");
            System.out.println();

            System.out.println("Loaded tables:");
            tableSizes.forEach((table, size) -> System.out.println(table + ": " + size));

            System.out.println();
            System.out.println("Violations:");
            if (violations.isEmpty()) {
                System.out.println("No violations");
            } else {
                System.out.println("Total: " + violationCounter);
                violations.forEach(pair -> System.out.println(pair.getLeft() + ": " + pair.getRight()));
            }

            System.out.println();
            System.out.println("################################################################################");
        }

        private void recordViolation(String violationId, String text, int count) {
            violationCounter += count;
            violations.add(Pair.of(violationId, text));
        }
    }
}
