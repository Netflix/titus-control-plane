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

package com.netflix.titus.api.audit.service;

import com.netflix.titus.api.audit.model.AuditLogEvent;
import rx.Observable;

/**
 * Audit logging API.
 */
public interface AuditLogService {

    /**
     * Submit new audit event. The event is added to an event queue, and it is guaranteed to immediately execute.
     */
    void submit(AuditLogEvent event);

    /**
     * Returns a hot observable to the audit event stream.
     */
    Observable<AuditLogEvent> auditLogEvents();
}
