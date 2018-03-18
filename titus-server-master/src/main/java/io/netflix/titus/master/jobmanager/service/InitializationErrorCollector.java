package io.netflix.titus.master.jobmanager.service;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Registry;
import io.netflix.titus.common.util.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class that tracks errors during jobs loading, and initialization process.
 */
class InitializationErrorCollector {

    private static final Logger logger = LoggerFactory.getLogger(InitializationErrorCollector.class);

    private final JobManagerConfiguration jobManagerConfiguration;

    private final AtomicInteger corruptedJobRecords = new AtomicInteger();
    private final AtomicInteger corruptedTaskRecords = new AtomicInteger();

    private final List<String> invalidJobs = new CopyOnWriteArrayList<>();
    private final List<String> strictlyInvalidJobs = new CopyOnWriteArrayList<>();
    private final List<String> invalidTasks = new CopyOnWriteArrayList<>();
    private final List<String> strictlyInvalidTasks = new CopyOnWriteArrayList<>();
    private final List<String> failedToAddToFenzoTask = new CopyOnWriteArrayList<>();
    private final List<String> inconsistentTasks = new CopyOnWriteArrayList<>();
    private final List<String> launchedTasksWithUnidentifiedAgents = new CopyOnWriteArrayList<>();
    private final List<Pair<String, Map<String, Set<String>>>> eniOverlaps = new CopyOnWriteArrayList<>();

    private final Gauge corruptedJobRecordsGauge;
    private final Gauge corruptedTaskRecordsGauge;
    private final Gauge invalidJobsGauge;
    private final Gauge strictlyInvalidJobsGauge;
    private final Gauge invalidTasksGauge;
    private final Gauge strictlyInvalidTasksGauge;
    private final Gauge failedToAddToFenzoTaskGauge;
    private final Gauge inconsistentTasksGauge;
    private final Gauge launchedTasksWithUnidentifiedAgentsGauge;
    private final Gauge eniOverlapsGauge;

    InitializationErrorCollector(JobManagerConfiguration jobManagerConfiguration, Registry registry) {
        this.jobManagerConfiguration = jobManagerConfiguration;

        this.corruptedJobRecordsGauge = registry.gauge(JobReconciliationFrameworkFactory.ROOT_METRIC_NAME + "corruptedJobRecords");
        this.corruptedTaskRecordsGauge = registry.gauge(JobReconciliationFrameworkFactory.ROOT_METRIC_NAME + "corruptedTaskRecords");
        this.invalidJobsGauge = registry.gauge(JobReconciliationFrameworkFactory.ROOT_METRIC_NAME + "invalidJobs");
        this.strictlyInvalidJobsGauge = registry.gauge(JobReconciliationFrameworkFactory.ROOT_METRIC_NAME + "strictlyInvalidJobs");
        this.invalidTasksGauge = registry.gauge(JobReconciliationFrameworkFactory.ROOT_METRIC_NAME + "invalidTasks");
        this.strictlyInvalidTasksGauge = registry.gauge(JobReconciliationFrameworkFactory.ROOT_METRIC_NAME + "strictlyInvalidTasks");
        this.failedToAddToFenzoTaskGauge = registry.gauge(JobReconciliationFrameworkFactory.ROOT_METRIC_NAME + "failedToAddToFenzoTask");
        this.inconsistentTasksGauge = registry.gauge(JobReconciliationFrameworkFactory.ROOT_METRIC_NAME + "inconsistentTasks");
        this.launchedTasksWithUnidentifiedAgentsGauge = registry.gauge(JobReconciliationFrameworkFactory.ROOT_METRIC_NAME + "launchedTasksWithUnidentifiedAgents");
        this.eniOverlapsGauge = registry.gauge(JobReconciliationFrameworkFactory.ROOT_METRIC_NAME + "eniOverlaps");
    }

    void corruptedJobRecords(int count) {
        corruptedJobRecords.addAndGet(count);
    }

    void invalidJob(String jobId) {
        invalidJobs.add(jobId);
    }

    public void strictlyInvalidJob(String jobId) {
        strictlyInvalidJobs.add(jobId);
    }

    void corruptedTaskRecords(int count) {
        corruptedTaskRecords.addAndGet(count);
    }

    void invalidTaskRecord(String taskId) {
        invalidTasks.add(taskId);
    }

    public void strictlyInvalidTask(String taskId) {
        strictlyInvalidTasks.add(taskId);
    }

    void taskAddToFenzoError(String taskId) {
        failedToAddToFenzoTask.add(taskId);
    }

    void inconsistentTask(String taskId) {
        inconsistentTasks.add(taskId);
    }

    void launchedTaskWithUnidentifiedAgent(String taskId) {
        launchedTasksWithUnidentifiedAgents.add(taskId);
    }

    void eniOverlaps(String eniSignature, Map<String, Set<String>> assignments) {
        eniOverlaps.add(Pair.of(eniSignature, assignments));
    }

    void failIfTooManyBadRecords() {
        writeStateToLog();
        createSpectatorMetrics();

        int allFailedJobs = corruptedJobRecords.get() + invalidJobs.size();

        int allFailedTasks = corruptedTaskRecords.get() + invalidTasks.size() + inconsistentTasks.size() + failedToAddToFenzoTask.size()
                + launchedTasksWithUnidentifiedAgents.size() + countEniAssignmentFailures();

        boolean failOnJobs = allFailedJobs > jobManagerConfiguration.getMaxFailedJobs();
        boolean failOnTasks = allFailedTasks > jobManagerConfiguration.getMaxFailedTasks();

        String jobErrorMessage = String.format("Exiting because the number of failed jobs (%s) was greater than allowed maximum (%s)", allFailedJobs, jobManagerConfiguration.getMaxFailedJobs());
        String taskErrorMessage = String.format("Exiting because the number of failed tasks (%s) was greater than allowed maximum (%s)", allFailedTasks, jobManagerConfiguration.getMaxFailedTasks());

        if (failOnJobs && failOnTasks) {
            logger.error(jobErrorMessage);
            logger.error(taskErrorMessage);
            throw new IllegalStateException(jobErrorMessage + ". " + taskErrorMessage);
        }
        if (failOnJobs) {
            logger.error(jobErrorMessage);
            throw new IllegalStateException(jobErrorMessage);
        }
        if (failOnTasks) {
            logger.error(taskErrorMessage);
            throw new IllegalStateException(taskErrorMessage);
        }

        if (allFailedJobs > 0) {
            logger.info("Ok to move on although bad job records found: badRecords={}, threshold={}", allFailedJobs, jobManagerConfiguration.getMaxFailedJobs());
        }
        if (allFailedTasks > 0) {
            logger.info("Ok to move on although bad task records found: badRecords={}, threshold={}", allFailedTasks, jobManagerConfiguration.getMaxFailedTasks());
        }
    }

    private void writeStateToLog() {
        if (corruptedJobRecords.get() > 0) {
            logger.info("Found corrupted job records: {}", corruptedJobRecords.get());
        }
        if (corruptedTaskRecords.get() > 0) {
            logger.info("Found corrupted task records: {}", corruptedTaskRecords.get());
        }
        if (!invalidJobs.isEmpty()) {
            logger.info("Found {} jobs with invalid state: {}", invalidJobs.size(), invalidJobs);
        }
        if (!strictlyInvalidJobs.isEmpty()) {
            logger.info("Found {} jobs with strictly invalid state: {}", strictlyInvalidJobs.size(), strictlyInvalidJobs);
        }
        if (!invalidTasks.isEmpty()) {
            logger.info("Found {} task with invalid state: {}", invalidTasks.size(), invalidTasks);
        }
        if (!strictlyInvalidTasks.isEmpty()) {
            logger.info("Found {} task with strictly invalid state: {}", strictlyInvalidTasks.size(), strictlyInvalidTasks);
        }
        if (!launchedTasksWithUnidentifiedAgents.isEmpty()) {
            logger.info("Found {} launched task with no agent assignment: {}", launchedTasksWithUnidentifiedAgents.size(), launchedTasksWithUnidentifiedAgents);
        }
        if (!eniOverlaps.isEmpty()) {
            logger.info("Found {} task with colliding ENI assignments: {}", countEniAssignmentFailures(), eniOverlaps);
        }
        if (!inconsistentTasks.isEmpty()) {
            logger.info("Found {} task with inconsistent state: {}", inconsistentTasks.size(), inconsistentTasks);
        }
        if (!failedToAddToFenzoTask.isEmpty()) {
            logger.info("Failed to add to Fenzo {} tasks: {}", failedToAddToFenzoTask.size(), failedToAddToFenzoTask);
        }
    }

    private void createSpectatorMetrics() {
        corruptedJobRecordsGauge.set(corruptedJobRecords.get());
        corruptedTaskRecordsGauge.set(corruptedTaskRecords.get());
        invalidJobsGauge.set(invalidJobs.size());
        strictlyInvalidJobsGauge.set(strictlyInvalidJobs.size());
        invalidTasksGauge.set(invalidTasks.size());
        strictlyInvalidTasksGauge.set(strictlyInvalidTasks.size());
        failedToAddToFenzoTaskGauge.set(failedToAddToFenzoTask.size());
        inconsistentTasksGauge.set(invalidTasks.size());
        launchedTasksWithUnidentifiedAgentsGauge.set(launchedTasksWithUnidentifiedAgents.size());
        eniOverlapsGauge.set(countEniAssignmentFailures());
    }

    private int countEniAssignmentFailures() {
        return eniOverlaps.stream().flatMapToInt(p -> p.getRight().values().stream().mapToInt(Set::size)).sum();
    }
}
