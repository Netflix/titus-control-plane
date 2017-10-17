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

package io.netflix.titus.common.jhiccup.sensor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.spectator.api.Registry;
import io.netflix.titus.common.jhiccup.HiccupRecorderConfiguration;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.SingleWriterRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Scheduler.Worker;
import rx.schedulers.Schedulers;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;

/**
 * Monitor RxJava computation event loop liveness.
 */
public class RxJavaComputationSchedulerSensor extends AbstractHiccupSensor {

    private static final Logger logger = LoggerFactory.getLogger(RxJavaComputationSchedulerSensor.class);

    private static final long MAX_INIT_PROCESSING_WAIT_TIME_MS = 5000;
    private static final int MAX_LOOP_COUNT = 100;

    private static final long DEFAULT_PROBING_INTERVAL_MS = 20;
    private static final long DEFAULT_PROGRESS_CHECK_INTERVAL_MS = 20;

    private static final long LOWEST_TRACKABLE_VALUE = 20000;
    private static final long HIGHEST_TRACKABLE_VALUE = 3600000000000L; // 1h
    private static final int NUMBER_OF_SIGNIFICANT_DIGITS = 2;

    private final List<WorkerTaskScheduler> workerTaskSchedulers;
    private final WorkerObserver workersObserver;
    private final long probingIntervalNs;
    private final long progressCheckIntervalMs;
    private volatile boolean doRun = true;

    @VisibleForTesting
    RxJavaComputationSchedulerSensor(HiccupRecorderConfiguration configuration,
                                     long probingIntervalMs,
                                     long progressCheckIntervalMs,
                                     Registry registry) {
        super(configuration, registry);
        this.probingIntervalNs = TimeUnit.MILLISECONDS.toNanos(probingIntervalMs);
        this.progressCheckIntervalMs = progressCheckIntervalMs;
        this.workerTaskSchedulers = findComputationSchedulerWorkers();
        this.workersObserver = new WorkerObserver();
        workersObserver.start();
    }

    public RxJavaComputationSchedulerSensor(HiccupRecorderConfiguration configuration, Registry registry) {
        this(configuration, DEFAULT_PROBING_INTERVAL_MS, DEFAULT_PROGRESS_CHECK_INTERVAL_MS, registry);
    }

    @Override
    public void reset() {
        workerTaskSchedulers.forEach(WorkerTaskScheduler::reset);
    }

    @Override
    public void shutdown() {
        if (doRun) {
            doRun = false;
            try {
                workersObserver.interrupt();
                workersObserver.join();
            } catch (InterruptedException ignore) {
            }
            workerTaskSchedulers.forEach(WorkerTaskScheduler::shutdown);
        }
    }

    public void report() {
        workerTaskSchedulers.forEach(WorkerTaskScheduler::report);
    }

    protected void reportBlockedThread(Thread blockedThread) {
        List<StackTraceElement> blockedStackTrace = asList(blockedThread.getStackTrace());

        // If JVM has a pause (for example due to GC) we may observe long delay, even if the computation queue is empty.
        // We cannot solve this problem for all cases, but we do not report blocked thread here if we observe that
        // computation thread is parked in executor pool.
        boolean free = blockedStackTrace.get(0).getMethodName().equals("park");
        for (int i = 1; free && i < blockedStackTrace.size(); i++) {
            free = blockedStackTrace.get(i).getClassName().startsWith("java.");
        }

        if (!free) {
            logger.warn("Rx computation scheduler thread {} blocked by long running task: {}", blockedThread.getName(), blockedStackTrace);
        }
    }

    @VisibleForTesting
    Map<String, Long> getLastPercentile(double percentile) {
        Map<String, Long> result = new HashMap<>();
        workerTaskSchedulers.forEach(w -> {
            result.put(w.getThreadName(), (long) w.report(percentile));
        });
        return result;
    }

    private List<WorkerTaskScheduler> findComputationSchedulerWorkers() {
        List<WorkerTaskScheduler> workerTaskSchedulers = new ArrayList<>();
        try {
            int cpuCount = Runtime.getRuntime().availableProcessors();
            Set<Thread> threads = new HashSet<>();
            int iterCount = 0;
            while (workerTaskSchedulers.size() < cpuCount && iterCount < MAX_LOOP_COUNT) {
                iterCount++;

                Worker worker = Schedulers.computation().createWorker();
                AtomicReference<Thread> workerThreadRef = new AtomicReference<>();
                CountDownLatch latch = new CountDownLatch(1);
                worker.schedule(() -> {
                    workerThreadRef.set(Thread.currentThread());
                    latch.countDown();
                });
                if (!latch.await(MAX_INIT_PROCESSING_WAIT_TIME_MS, TimeUnit.MILLISECONDS)) {
                    logger.warn("RxJava computation scheduler sensor initialization error");
                    return Collections.emptyList();
                }
                if (threads.add(workerThreadRef.get())) {
                    workerTaskSchedulers.add(new WorkerTaskScheduler(workerThreadRef.get(), worker));
                } else {
                    worker.unsubscribe();
                }
            }
            return workerTaskSchedulers;
        } catch (Exception e) {
            logger.warn("RxJava computation scheduler sensor initialization error", e);
            workerTaskSchedulers.forEach(WorkerTaskScheduler::shutdown);
            return Collections.emptyList();
        }
    }

    /**
     * Test task runner on a single Rx computation thread.
     */
    private class WorkerTaskScheduler {
        private final Worker worker;
        private final SingleWriterRecorder recorder;
        private final Thread workerThread;

        private volatile Histogram lastReturnedHistogram;
        private volatile long pendingTaskStartTimeNs;

        private final HiccupSensorMetrics metrics;

        private long shortestObservedDeltaNs = Long.MAX_VALUE;

        WorkerTaskScheduler(Thread workerThread, Worker worker) {
            this.worker = worker;
            this.workerThread = workerThread;
            this.metrics = new HiccupSensorMetrics("titus.hiccup.computationScheduler", singletonMap("thread", workerThread.getName()), registry);
            this.recorder = new SingleWriterRecorder(LOWEST_TRACKABLE_VALUE, HIGHEST_TRACKABLE_VALUE, NUMBER_OF_SIGNIFICANT_DIGITS);

            registry.gauge(
                    registry.createId("titus.hiccup.computationScheduler.pendingTaskDuration", singletonMap("thread", workerThread.getName())),
                    0, v -> getPendingTaskRunningTime()
            );

            doRun();
        }

        String getThreadName() {
            return workerThread.getName();
        }

        Thread getWorkerThread() {
            return workerThread;
        }

        long getPendingTaskRunningTime() {
            return TimeUnit.NANOSECONDS.toMillis(Math.max(0, System.nanoTime() - pendingTaskStartTimeNs - probingIntervalNs));
        }

        long getPendingTaskStartTimeNs() {
            return pendingTaskStartTimeNs;
        }

        void reset() {
            recorder.reset();
        }

        void report() {
            report(99.5);
        }

        double report(double percentile) {
            lastReturnedHistogram = recorder.getIntervalHistogram(lastReturnedHistogram);
            if (lastReturnedHistogram.getTotalCount() > 0) {
                metrics.updateMetrics(lastReturnedHistogram);
            }
            return TimeUnit.NANOSECONDS.toMillis(lastReturnedHistogram.getValueAtPercentile(percentile));
        }

        void shutdown() {
            worker.unsubscribe();
        }

        private void doRun() {
            pendingTaskStartTimeNs = System.nanoTime();
            worker.schedule(() -> {
                long deltaNs = System.nanoTime() - pendingTaskStartTimeNs;
                shortestObservedDeltaNs = Math.min(shortestObservedDeltaNs, deltaNs);
                long hiccupNs = deltaNs - shortestObservedDeltaNs;
                try {
                    recorder.recordValueWithExpectedInterval(hiccupNs, probingIntervalNs);
                } catch (Exception e) {
                    logger.warn("Could not update histogram of RxComputation thread {}: {}", workerThread.getName(), e.getMessage());
                }
                doRun();
            }, probingIntervalNs, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * This thread observes progress of work on the individual Rx compute threads, to detect event loop halts.
     */
    private class WorkerObserver extends Thread {

        private final Map<WorkerTaskScheduler, Long> blockedTasks = new ConcurrentHashMap<>();

        WorkerObserver() {
            super("rxHiccupSensor");
            this.setDaemon(true);
        }

        @Override
        public void run() {
            try {
                while (doRun) {
                    checkProgress();
                    try {
                        Thread.sleep(progressCheckIntervalMs);
                    } catch (InterruptedException ignore) {
                    }
                }
            } finally {
                logger.info("Leaving WorkerObserver execution loop");
            }
        }

        private void checkProgress() {
            workerTaskSchedulers.forEach(ts -> {
                long taskRunningTime = ts.getPendingTaskRunningTime();
                if (taskRunningTime > configuration.getTaskExecutionDeadlineMs()) {
                    Long lastReport = blockedTasks.get(ts);
                    long pendingTaskStartTimeNs = ts.getPendingTaskStartTimeNs();
                    if (lastReport == null || lastReport != pendingTaskStartTimeNs) {
                        blockedTasks.put(ts, pendingTaskStartTimeNs);
                        reportBlockedThread(ts.getWorkerThread());
                    }
                }
            });
        }
    }
}
