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

import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.audit.model.AuditLogEvent;
import com.netflix.titus.api.audit.service.AuditLogService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Subscriber;

/**
 * Writes audit events to a logger.
 */
@Singleton
public class AuditEventLogger {

    private static final Logger logger = LoggerFactory.getLogger(AuditEventLogger.class);

    @Inject
    public AuditEventLogger(AuditLogService auditLogService) {
        auditLogService.auditLogEvents().subscribe(
                new Subscriber<AuditLogEvent>() {
                    @Override
                    public void onCompleted() {
                        logger.warn("Audit log observable completed");
                    }

                    @Override
                    public void onError(Throwable e) {
                        logger.error("Error in Audit long observable: {}", e.getMessage(), e);
                    }

                    @Override
                    public void onNext(AuditLogEvent event) {
                        logger.info("Audit log: event type: {}, operand: {}, data: {}", event.getType(), event.getOperand(), event.getData());
                    }
                }
        );
    }
}
