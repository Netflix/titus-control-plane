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

package com.netflix.titus.master.store;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.master.MetricConstants;
import com.netflix.titus.master.Status;
import com.netflix.titus.master.job.worker.internal.DefaultWorkerStateMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.BackpressureOverflow;
import rx.Observable;
import rx.Subscription;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;
import rx.subjects.SerializedSubject;
import rx.subjects.Subject;

/**
 * Worker state observer relays worker status events into an observable with back-pressure support
 */
@Singleton
public class WorkerStateObserverImpl implements WorkerStateObserver {
    private static final Logger logger = LoggerFactory.getLogger(WorkerStateObserverImpl.class);

    private static final int BACK_PRESSURE_BUFFER_SIZE = 10000;

    private final Subject<Status, Status> observer;
    private final Observable<Status> observable;
    private final Subscription workerStateMonitorSubscription;
    private final Counter overflowCounter;
    private final Counter workerMonitorSubscriptionError;


    @Inject
    public WorkerStateObserverImpl(DefaultWorkerStateMonitor defaultWorkerStateMonitor, Registry registry) {
        overflowCounter = registry.counter(MetricConstants.METRIC_WORKER_STATE_OBSERVER + "overflow");
        workerMonitorSubscriptionError = registry.counter(MetricConstants.METRIC_WORKER_STATE_OBSERVER +
                "monitorSubscriptionError");
        this.observer = new SerializedSubject<>(PublishSubject.create());
        this.observable = observer
                .onBackpressureBuffer(BACK_PRESSURE_BUFFER_SIZE,
                        () -> {
                            logger.warn("Exceeded back pressure buffer of " + BACK_PRESSURE_BUFFER_SIZE);
                            overflowCounter.increment();
                        },
                        BackpressureOverflow.ON_OVERFLOW_DROP_OLDEST)
                .observeOn(Schedulers.io());

        workerStateMonitorSubscription = defaultWorkerStateMonitor.getAllStatusObservable()
                .subscribe(s -> {
                            observer.onNext(s);
                        },
                        e -> {
                            workerMonitorSubscriptionError.increment();
                            logger.error("Error from DefaultWorkerStateMonitor ", e);
                        }
                );

    }

    @PreDestroy
    public void cleanup() {
        workerStateMonitorSubscription.unsubscribe();
    }

    @Override
    public Observable<Status> getObservable() {
        return this.observable;
    }
}
