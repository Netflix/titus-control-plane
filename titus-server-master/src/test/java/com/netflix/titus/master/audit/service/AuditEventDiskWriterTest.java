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

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.netflix.titus.api.audit.model.AuditLogEvent;
import com.netflix.titus.api.audit.model.AuditLogEvent.Type;
import com.netflix.titus.api.audit.service.AuditLogService;
import com.netflix.titus.api.model.event.UserRequestEvent;
import com.netflix.titus.common.util.IOExt;
import com.netflix.titus.common.util.rx.eventbus.RxEventBus;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;
import rx.subjects.PublishSubject;

import static com.netflix.titus.master.audit.service.AuditEventDiskWriter.LOG_FILE_NAME;
import static com.netflix.titus.master.audit.service.AuditEventDiskWriter.WRITE_INTERVAL_MS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AuditEventDiskWriterTest {

    private static final String LOG_FOLDER = "build/auditLogs";
    private static final File LOG_FILE = new File(LOG_FOLDER, LOG_FILE_NAME);

    private TestScheduler testScheduler = Schedulers.test();

    private final RxEventBus rxEventBus = mock(RxEventBus.class);
    private final AuditLogConfiguration config = mock(AuditLogConfiguration.class);
    private final AuditLogService auditLogService = mock(AuditLogService.class);

    private AuditEventDiskWriter auditEventDiskWriter;
    private PublishSubject<AuditLogEvent> eventSubject = PublishSubject.create();
    private PublishSubject<UserRequestEvent> rxEventSubject = PublishSubject.create();

    @Before
    public void setUp() throws Exception {
        LOG_FILE.delete();
        assertThat(LOG_FILE.exists()).isFalse();

        when(config.getAuditLogFolder()).thenReturn(LOG_FOLDER);
        when(auditLogService.auditLogEvents()).thenReturn(eventSubject);
        when(rxEventBus.listen(AuditEventDiskWriter.class.getSimpleName(), UserRequestEvent.class)).thenReturn(rxEventSubject);
        auditEventDiskWriter = new AuditEventDiskWriter(config, auditLogService, rxEventBus, testScheduler);
    }

    @After
    public void tearDown() throws Exception {
        auditEventDiskWriter.shutdown();
    }

    @Test
    public void testLogWrite() throws Exception {
        eventSubject.onNext(createEvent());
        rxEventSubject.onNext(createHttpEvent());
        testScheduler.advanceTimeBy(WRITE_INTERVAL_MS, TimeUnit.MILLISECONDS);

        List<String> lines = IOExt.readLines(LOG_FILE);
        assertThat(lines).hasSize(2);
    }

    private AuditLogEvent createEvent() {
        return new AuditLogEvent(Type.JOB_SUBMIT, "operand", "data", System.currentTimeMillis());
    }

    private UserRequestEvent createHttpEvent() {
        return new UserRequestEvent("POST /api/v2/jobs", "userX", "jobId=123", System.currentTimeMillis());
    }
}