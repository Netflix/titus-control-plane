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

import javax.inject.Inject;
import javax.inject.Singleton;

import io.netflix.titus.api.audit.model.AuditLogEvent;
import io.netflix.titus.api.audit.service.AuditLogService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.BackpressureOverflow;
import rx.Observable;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;
import rx.subjects.SerializedSubject;
import rx.subjects.Subject;

@Singleton
public class DefaultAuditLogService implements AuditLogService {

    private static final Logger logger = LoggerFactory.getLogger(DefaultAuditLogService.class);

    private static final int BACK_PRESSURE_BUFFER_SIZE = 10000;

    private final Subject<AuditLogEvent, AuditLogEvent> observer;
    private final Observable<AuditLogEvent> observable;

    @Inject
    public DefaultAuditLogService() {
        this.observer = new SerializedSubject<>(PublishSubject.create());
        this.observable = observer
                .onBackpressureBuffer(
                        BACK_PRESSURE_BUFFER_SIZE,
                        () -> logger.warn("Exceeded back pressure buffer of " + BACK_PRESSURE_BUFFER_SIZE),
                        BackpressureOverflow.ON_OVERFLOW_DROP_OLDEST
                )
                .observeOn(Schedulers.io());
    }

    @Override
    public void submit(AuditLogEvent event) {
        observer.onNext(event);
    }

    @Override
    public Observable<AuditLogEvent> auditLogEvents() {
        return observable;
    }
}
