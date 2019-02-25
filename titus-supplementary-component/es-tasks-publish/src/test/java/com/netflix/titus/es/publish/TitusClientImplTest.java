package com.netflix.titus.es.publish;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.titus.es.publish.config.EsPublisherConfiguration;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.assertj.core.api.Java6Assertions.fail;

@Category(IntegrationTest.class)
public class TitusClientImplTest {

    private static EsPublisherConfiguration getConfig() {
        return new EsPublisherConfiguration("us-east-1", "test", "test",
                "", "", true);
    }

    private static TitusClient titusClient;
    private static String TASK_ID = "7c10a5e0-2811-4d4a-9db2-08d883b5c766";
    private static String JOB_ID = "75df0af3-d896-45af-87a6-21d3bfce5480";

    @BeforeClass
    public static void setup() {
        final TitusClientUtils titusClientUtils = new TitusClientUtils(getConfig());
        titusClient = new TitusClientImpl(TestUtils.buildTitusGrpcChannel(titusClientUtils), new DefaultRegistry());
    }

    @Test
    public void getTaskById() {
        final CountDownLatch latch = new CountDownLatch(1);
        titusClient.getTask(TASK_ID).subscribe(task -> {
            assertThat(task.getId()).isEqualTo(TASK_ID);
            latch.countDown();
        });
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            fail("getTaskById Timeout ", e);
        }
    }

    @Test
    public void getJobById() {
        final CountDownLatch latch = new CountDownLatch(1);
        titusClient.getJobById(JOB_ID).subscribe(job -> {
            assertThat(job.getId()).isEqualTo(JOB_ID);
            latch.countDown();
        });
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            fail("getJobById Timeout ", e);
        }
    }

    @Test
    public void getRunningTasks() {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicInteger tasksCount = new AtomicInteger(0);
        titusClient.getRunningTasks().subscribe(task -> tasksCount.incrementAndGet(),
                e -> fail("getRunningTasks exception {}", e),
                () -> {
                    latch.countDown();
                    assertThat(tasksCount.get()).isGreaterThan(0);
                });
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            fail("getRunningTasks Timeout ", e);
        }
    }

    @Test
    public void getTaskUpdates() {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicInteger tasksCount = new AtomicInteger(0);
        titusClient.getTaskUpdates().subscribe(task -> {
            if (tasksCount.incrementAndGet() == 10) {
                latch.countDown();
            }
        }, e -> fail("getTaskUpdates exception {}", e));
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            fail("getTaskUpdates Timeout ", e);
        }
    }
}

