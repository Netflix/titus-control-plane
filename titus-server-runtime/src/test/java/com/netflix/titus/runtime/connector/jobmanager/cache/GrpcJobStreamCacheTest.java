package com.netflix.titus.runtime.connector.jobmanager.cache;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.runtime.connector.jobmanager.JobCache;
import com.netflix.titus.runtime.connector.jobmanager.JobManagementClient;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import com.netflix.titus.testkit.model.job.JobGeneratorOrchestrator;
import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.Before;
import org.junit.Test;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GrpcJobStreamCacheTest {

    private static final String SERVICE_JOB = "serviceJob";
    private static final String BATCH_JOB = "batchJob";

    private static final int SERVICE_DESIRED = 5;
    private static final int BATCH_DESIRED = 1;

    private final TestScheduler testScheduler = Schedulers.test();

    private final TitusRuntime titusRuntime = TitusRuntimes.test(testScheduler);

    private final JobGeneratorOrchestrator dataGenerator = new JobGeneratorOrchestrator(titusRuntime);

    private final JobManagementClient client = mock(JobManagementClient.class);

    private final GrpcJobStreamCache jobStreamCache = new GrpcJobStreamCache(client, titusRuntime, testScheduler);

    private final ExtTestSubscriber<JobStreamCache.CacheEvent> cacheEventSubscriber = new ExtTestSubscriber<>();

    @Before
    public void setUp() {
        when(client.observeJobs()).thenReturn(dataGenerator.grpcObserveJobs(true));

        dataGenerator.addJobTemplate(SERVICE_JOB, JobDescriptorGenerator.serviceJobDescriptors()
                .map(jd -> jd.but(d -> d.getExtensions().toBuilder().withCapacity(Capacity.newBuilder().withDesired(SERVICE_DESIRED).withMax(10).build())))
                .cast(JobDescriptor.class)
        );

        dataGenerator.addJobTemplate(BATCH_JOB, JobDescriptorGenerator.batchJobDescriptors()
                .map(jd -> jd.but(d -> d.getExtensions().toBuilder().withSize(BATCH_DESIRED)))
                .cast(JobDescriptor.class)
        );
    }

    @Test
    public void testCacheBootstrap() {
        dataGenerator.creteMultipleJobsAndTasks(SERVICE_JOB, BATCH_JOB);
        jobStreamCache.connect().subscribe(cacheEventSubscriber);

        JobStreamCache.CacheEvent initialCacheEvent = cacheEventSubscriber.takeNext();
        assertThat(initialCacheEvent).isNotNull();

        JobCache cache = initialCacheEvent.getCache();
        assertThat(cache.getJobs()).hasSize(2);
        assertThat(cache.getTasks()).hasSize(SERVICE_DESIRED + BATCH_DESIRED);
    }

    @Test
    public void testCacheJobUpdate() {
        Job job = bootstrapWithOneJobNoTasks();
        dataGenerator.moveJobToKillInitiatedState(job);
        assertThat(cacheEventSubscriber.takeNext().getCache().getJobs().get(0).getStatus().getState()).isEqualTo(JobState.KillInitiated);
    }

    @Test
    public void testCacheJobRemove() {
        Job job = bootstrapWithOneJobNoTasks();
        dataGenerator.moveJobToKillInitiatedState(job);
        cacheEventSubscriber.skipAvailable();

        dataGenerator.finishJob(job);
        assertThat(cacheEventSubscriber.takeNext().getCache().getJobs()).isEmpty();
    }

    @Test
    public void testCacheTaskUpdate() {
        Pair<Job, List<Task>> pair = bootstrapWithOneJobAndOneTask();
        Task task = pair.getRight().get(0);
        dataGenerator.moveTaskToState(task, TaskState.Launched);
        assertThat(cacheEventSubscriber.takeNext().getCache().getTasks().get(0).getStatus().getState()).isEqualTo(TaskState.Launched);
    }

    @Test
    public void testCacheTaskRemove() {
        Pair<Job, List<Task>> pair = bootstrapWithOneJobAndOneTask();
        Task task = pair.getRight().get(0);

        dataGenerator.moveTaskToState(task, TaskState.Finished);
        assertThat(cacheEventSubscriber.takeNext().getCache().getTasks()).isEmpty();
    }

    @Test
    public void testReemit() {
        dataGenerator.creteMultipleJobsAndTasks(SERVICE_JOB, BATCH_JOB);
        jobStreamCache.connect().subscribe(cacheEventSubscriber);

        assertThat(cacheEventSubscriber.takeNext()).isNotNull();

        assertThat(cacheEventSubscriber.takeNext()).isNull();
        testScheduler.advanceTimeBy(GrpcJobStreamCache.LATENCY_REPORT_INTERVAL_MS, TimeUnit.MILLISECONDS);
        assertThat(cacheEventSubscriber.takeNext()).isNotNull();
    }

    private Job bootstrapWithOneJobNoTasks() {
        Job job = dataGenerator.createJob(SERVICE_JOB);
        jobStreamCache.connect().subscribe(cacheEventSubscriber);
        cacheEventSubscriber.skipAvailable();
        return job;
    }

    private Pair<Job, List<Task>> bootstrapWithOneJobAndOneTask() {
        Pair<Job, List<Task>> pair = dataGenerator.createJobAndTasks(BATCH_JOB);
        jobStreamCache.connect().subscribe(cacheEventSubscriber);
        cacheEventSubscriber.skipAvailable();
        return pair;
    }
}