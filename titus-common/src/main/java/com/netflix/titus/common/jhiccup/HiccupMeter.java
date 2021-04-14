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

package com.netflix.titus.common.jhiccup;

import java.util.Arrays;
import java.util.List;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.spectator.api.Registry;
import com.netflix.titus.common.jhiccup.sensor.HiccupSensor;
import com.netflix.titus.common.jhiccup.sensor.JvmHiccupSensor;
import com.netflix.titus.common.jhiccup.sensor.RxJavaComputationSchedulerSensor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JVM and event pool hiccups monitoring (inspired by jHiccup from http://www.azul.com/jhiccup/).
 */
@Singleton
public class HiccupMeter {

    private static final Logger logger = LoggerFactory.getLogger(HiccupMeter.class);

    /*
     * Store configuration in object, instead of calling HiccupRecorderConfiguration on each invocation
     * which may be expensive (especially for Spring Environment).
     */
    private final long reportingIntervalMs;
    private final long startDelayMs;

    private final HiccupMeterController controller;
    private volatile boolean doRun = true;

    private final List<HiccupSensor> sensors;

    @Inject
    public HiccupMeter(HiccupRecorderConfiguration configuration, Registry registry) {
        this.reportingIntervalMs = configuration.getReportingIntervalMs();
        this.startDelayMs = configuration.getStartDelayMs();
        this.sensors = Arrays.asList(
                new JvmHiccupSensor(configuration, registry),
                new RxJavaComputationSchedulerSensor(configuration, registry)
        );
        this.controller = new HiccupMeterController();
        this.controller.start();
    }

    @PreDestroy
    public void shutdown() {
        doRun = false;
        sensors.forEach(HiccupSensor::shutdown);
        if (controller.isAlive()) {
            try {
                controller.join();
            } catch (InterruptedException e) {
                logger.warn("HiccupMeter terminate/join interrupted");
            }
        }
    }

    class HiccupMeterController extends Thread {

        HiccupMeterController() {
            this.setName("jhiccup");
            this.setDaemon(true);
        }

        @Override
        public void run() {
            try {
                // Warmup
                if (startDelayMs > 0) {
                    Thread.sleep(startDelayMs);
                }

                // Main loop
                runLoop();
            } catch (InterruptedException e) {
                logger.warn("HiccupMeter terminating...");
            }

            sensors.forEach(HiccupSensor::shutdown);
        }

        private void runLoop() throws InterruptedException {
            sensors.forEach(HiccupSensor::reset);

            long now = System.currentTimeMillis();
            long nextReportingTime = now + reportingIntervalMs;

            while (doRun) {
                waitTillNextReporting(nextReportingTime);
                sensors.forEach(HiccupSensor::report);
                nextReportingTime += reportingIntervalMs;
            }
        }

        private void waitTillNextReporting(long nextReportingTime) throws InterruptedException {
            long now = System.currentTimeMillis();
            if (now < nextReportingTime) {
                Thread.sleep(nextReportingTime - now);
            }
        }
    }
}
