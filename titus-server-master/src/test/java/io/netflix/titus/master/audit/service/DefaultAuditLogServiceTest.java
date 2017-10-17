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

package io.netflix.titus.master.audit.service;

import java.util.concurrent.TimeUnit;

import io.netflix.titus.api.audit.model.AuditLogEvent;
import io.netflix.titus.master.audit.service.DefaultAuditLogService;
import io.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DefaultAuditLogServiceTest {

    private static final int TIMEOUT_MS = 5000;

    private final DefaultAuditLogService auditLogService = new DefaultAuditLogService();

    @Test
    public void testEventPropagation() throws Exception {
        ExtTestSubscriber<AuditLogEvent> testSubscriber = new ExtTestSubscriber<>();
        auditLogService.auditLogEvents().subscribe(testSubscriber);

        auditLogService.submit(createEvent());

        assertThat(testSubscriber.takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS)).isNotNull();
    }

    private AuditLogEvent createEvent() {
        return new AuditLogEvent(AuditLogEvent.Type.JOB_SUBMIT, "operand", "data", System.currentTimeMillis());
    }
}