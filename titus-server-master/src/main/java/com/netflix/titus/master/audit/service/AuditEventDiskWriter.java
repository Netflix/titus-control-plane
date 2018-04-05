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

package com.netflix.titus.master.audit.service;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.audit.model.AuditLogEvent;
import com.netflix.titus.api.audit.service.AuditLogService;
import com.netflix.titus.api.model.event.UserRequestEvent;
import com.netflix.titus.common.util.DateTimeExt;
import com.netflix.titus.common.util.IOExt;
import com.netflix.titus.common.util.rx.eventbus.RxEventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.schedulers.Schedulers;

/**
 * Audit logging to a file.
 */
@Singleton
public class AuditEventDiskWriter {

    private static final Logger logger = LoggerFactory.getLogger(AuditEventDiskWriter.class);

    static final String LOG_FILE_NAME = "titus-audit.log";

    static final long WRITE_INTERVAL_MS = 1000;

    private final File auditLogFolder;
    private final RxEventBus rxEventBus;
    private final File auditLogFile;

    private final AuditLogService auditLogService;
    private final Scheduler ioScheduler;

    private final Subscription auditLogSubscription;
    private final Subscription rxEventsSubscription;
    private final Subscription logWriterSubscription;

    private final Queue<AuditLogEvent> eventQueue = new LinkedBlockingDeque<>();
    private final Queue<UserRequestEvent> eventBusQueue = new LinkedBlockingQueue<>();
    private volatile Writer logWriter;

    @Inject
    public AuditEventDiskWriter(AuditLogConfiguration config, AuditLogService auditLogService, RxEventBus rxEventBus) {
        this(config, auditLogService, rxEventBus, Schedulers.io());
    }

    public AuditEventDiskWriter(AuditLogConfiguration config, AuditLogService auditLogService, RxEventBus rxEventBus, Scheduler ioScheduler) {
        this.auditLogFolder = createAuditLogFolder(new File(config.getAuditLogFolder()));
        this.rxEventBus = rxEventBus;
        this.auditLogFile = new File(auditLogFolder, LOG_FILE_NAME);
        this.auditLogService = auditLogService;
        this.ioScheduler = ioScheduler;
        this.auditLogSubscription = enableLogging();
        this.rxEventsSubscription = enableRxEventBusLogging();
        this.logWriterSubscription = enableLogWriter();
    }

    @PreDestroy
    public void shutdown() {
        auditLogSubscription.unsubscribe();
        rxEventsSubscription.unsubscribe();
        logWriterSubscription.unsubscribe();
        shutdownInternal();
    }

    private void shutdownInternal() {
        writeLog();
        IOExt.closeSilently(logWriter);
    }

    private Subscription enableLogging() {
        return auditLogService.auditLogEvents()
                .doOnTerminate(() -> logger.info("Terminating audit log subscription"))
                .subscribe(eventQueue::add);
    }

    private Subscription enableRxEventBusLogging() {
        return rxEventBus.listen(getClass().getSimpleName(), UserRequestEvent.class)
                .doOnTerminate(() -> logger.info("Terminating RxEventBus subscription"))
                .subscribe(eventBusQueue::add);
    }

    private Subscription enableLogWriter() {
        return Observable.interval(0, WRITE_INTERVAL_MS, TimeUnit.MILLISECONDS, ioScheduler)
                .doOnTerminate(() -> {
                    logger.info("Terminating audit log writer");
                    shutdownInternal();
                })
                .subscribe(tick -> writeLog());
    }

    private void writeLog() {
        try {
            if (logWriter == null) {
                createAuditLogFolder(auditLogFolder);
                logWriter = new BufferedWriter(new FileWriter(auditLogFile, true));
            }
            for (AuditLogEvent event = eventQueue.poll(); event != null; event = eventQueue.poll()) {
                logWriter.write(formatEvent(event));
                logWriter.write('\n');
            }
            for (UserRequestEvent event = eventBusQueue.poll(); event != null; event = eventBusQueue.poll()) {
                logWriter.write(formatEvent(event));
                logWriter.write('\n');
            }

            logWriter.flush();
        } catch (Exception e) {
            logger.warn("Audit log write to disk failure ({})", e.getMessage());
        }
    }

    private static File createAuditLogFolder(File logFolder) {
        if (!logFolder.exists()) {
            if (!logFolder.mkdirs()) {
                throw new IllegalArgumentException("Cannot create an audit log folder " + logFolder);
            }
        } else {
            if (!logFolder.isDirectory()) {
                throw new IllegalArgumentException(logFolder + " is configured as an audit log folder, but it is not a directory");
            }
        }

        return logFolder;
    }

    private String formatEvent(AuditLogEvent event) {
        StringBuilder sb = new StringBuilder();
        sb.append(DateTimeExt.toUtcDateTimeString(event.getTime()));
        sb.append(",source=JobManager,");
        sb.append(event.getType());
        sb.append(',');
        sb.append(event.getOperand());
        sb.append(',');
        sb.append(event.getData());
        return sb.toString();
    }

    private String formatEvent(UserRequestEvent event) {
        StringBuilder sb = new StringBuilder();
        sb.append(DateTimeExt.toUtcDateTimeString(event.getTimestamp()));
        sb.append(",source=HTTP,");
        sb.append(event.getOperation());
        sb.append(",callerId=");
        sb.append(event.getCallerId());
        sb.append(',');
        sb.append(event.getDetails());
        return sb.toString();
    }
}
