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

package com.netflix.titus.common.jhiccup.sensor;

import java.util.concurrent.TimeUnit;

import com.netflix.spectator.api.Registry;
import com.netflix.titus.common.jhiccup.HiccupRecorderConfiguration;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.SingleWriterRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JVM hiccup monitor.
 */
public class JvmHiccupSensor extends AbstractHiccupSensor implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(JvmHiccupSensor.class);

    private static final long LOWEST_TRACKABLE_VALUE = 20000;
    private static final long HIGHEST_TRACKABLE_VALUE = 3600000000000L; // 1h
    private static final int NUMBER_OF_SIGNIFICANT_DIGITS = 2;
    private static final long SAMPLING_RESOLUTION_NS = 1000_000;

    private final SingleWriterRecorder recorder;
    private final Thread executor;
    private final HiccupSensorMetrics metrics;

    private volatile boolean doRun = true;
    private volatile Histogram lastReturnedHistogram;

    public volatile String nonOptimizableRef; // public volatile so it is not optimized away

    public JvmHiccupSensor(HiccupRecorderConfiguration configuration, Registry registry) {
        super(configuration, registry);
        this.executor = new Thread(this, "jvmHiccupSensor");
        this.executor.setDaemon(true);
        this.recorder = new SingleWriterRecorder(LOWEST_TRACKABLE_VALUE, HIGHEST_TRACKABLE_VALUE, NUMBER_OF_SIGNIFICANT_DIGITS);
        this.metrics = new HiccupSensorMetrics("titus.hiccup.jvm", registry);

        executor.start();
    }

    @Override
    public void reset() {
        recorder.reset();
    }

    @Override
    public void report() {
        lastReturnedHistogram = recorder.getIntervalHistogram(lastReturnedHistogram);
        if (lastReturnedHistogram.getTotalCount() > 0) {
            metrics.updateMetrics(lastReturnedHistogram);
        }
    }

    @Override
    public void shutdown() {
        this.doRun = false;
        try {
            executor.join();
        } catch (InterruptedException ignore) {
        }
    }

    public void run() {
        try {
            long shortestObservedDeltaNs = Long.MAX_VALUE;
            while (doRun) {
                long startTime = System.nanoTime();
                TimeUnit.NANOSECONDS.sleep(SAMPLING_RESOLUTION_NS);
                nonOptimizableRef = Long.toString(startTime); // Force heap allocation

                long deltaNs = System.nanoTime() - startTime;
                shortestObservedDeltaNs = Math.min(shortestObservedDeltaNs, deltaNs);

                recorder.recordValueWithExpectedInterval(deltaNs - shortestObservedDeltaNs, SAMPLING_RESOLUTION_NS);
            }
        } catch (InterruptedException e) {
            logger.warn("JvmHiccupSensor interrupted/terminating...");
        }
    }
}