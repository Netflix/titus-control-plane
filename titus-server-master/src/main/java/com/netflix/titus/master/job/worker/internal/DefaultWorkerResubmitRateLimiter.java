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

package com.netflix.titus.master.job.worker.internal;

import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.master.config.MasterConfiguration;
import com.netflix.titus.master.job.worker.WorkerResubmitRateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.Scheduler;
import rx.Subscriber;
import rx.functions.Action0;
import rx.observers.SerializedObserver;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;

@Singleton
public class DefaultWorkerResubmitRateLimiter implements WorkerResubmitRateLimiter {

    private static final Logger logger = LoggerFactory.getLogger(DefaultWorkerResubmitRateLimiter.class);

    static class ResubmitRecord {
        private final String workerId;
        private final long resubmitAt;
        private final long delayedBy;

        private ResubmitRecord(String workerId, long resubmitAt, long delayedBy) {
            this.workerId = workerId;
            this.resubmitAt = resubmitAt;
            this.delayedBy = delayedBy;
        }

        /* package */ long getDelayedBy() {
            return delayedBy;
        }
    }

    private static class ResubmitRequest {
        private final String jobId;
        private final int stage;
        private final int index;

        private ResubmitRequest(String jobId, int stage, int index) {
            this.jobId = jobId;
            this.stage = stage;
            this.index = index;
        }
    }

    private class ResubmitRateLimiterOperator<T, R> implements Observable.Operator<String, ResubmitRequest> {
        private long EXPIRE_RESUBMIT_DELAY_SECS = 300;
        private long expireResubmitDelayExecutionIntervalSecs = 120;
        private final long defaultResubmissionIntervalSecs = 10;
        private final long[] resubmitIntervalSecs;
        private final String jobId;
        private final ConcurrentMap<String, ResubmitRecord> resubmitRecords;

        ResubmitRateLimiterOperator(String jobId) {
            this.jobId = jobId;
            resubmitRecords = new ConcurrentHashMap<>();
            resubmitRecordsMap.put(jobId, resubmitRecords);
            String workerResubmitIntervalSecs = config.getWorkerResubmitIntervalSecs();
            if (workerResubmitIntervalSecs == null || workerResubmitIntervalSecs.isEmpty()) {
                workerResubmitIntervalSecs = "5:10:20";
            }
            StringTokenizer tokenizer = new StringTokenizer(workerResubmitIntervalSecs, ":");
            if (tokenizer.countTokens() == 0) {
                resubmitIntervalSecs = new long[2];
                resubmitIntervalSecs[0] = 0L;
                resubmitIntervalSecs[1] = defaultResubmissionIntervalSecs;
            } else {
                resubmitIntervalSecs = new long[tokenizer.countTokens() + 1];
                resubmitIntervalSecs[0] = 0L;
                for (int i = 1; i < resubmitIntervalSecs.length; i++) {
                    final String s = tokenizer.nextToken();
                    try {
                        resubmitIntervalSecs[i] = Long.parseLong(s);
                    } catch (NumberFormatException e) {
                        logger.warn("Invalid number for resubmit interval " + s + ": using default " + defaultResubmissionIntervalSecs);
                        resubmitIntervalSecs[i] = defaultResubmissionIntervalSecs;
                    }
                }
            }
            EXPIRE_RESUBMIT_DELAY_SECS = config.getExpireWorkerResubmitDelaySecs();
            expireResubmitDelayExecutionIntervalSecs = config.getExpireResubmitDelayExecutionIntervalSecs();
        }

        @Override
        public Subscriber<? super ResubmitRequest> call(final Subscriber<? super String> child) {
            child.add(Schedulers.computation().createWorker().schedulePeriodically(new Action0() {
                @Override
                public void call() {
                    for (String workerId : resubmitRecords.keySet()) {
                        final ResubmitRecord record = resubmitRecords.get(workerId);
                        if (record.resubmitAt - record.delayedBy < (scheduler.now() - EXPIRE_RESUBMIT_DELAY_SECS * 1000)) {
                            resubmitRecords.remove(workerId);
                        }
                    }
                }
            }, expireResubmitDelayExecutionIntervalSecs, expireResubmitDelayExecutionIntervalSecs, TimeUnit.SECONDS));
            return new Subscriber<ResubmitRequest>() {
                @Override
                public void onCompleted() {
                }

                @Override
                public void onError(Throwable e) {
                }

                @Override
                public void onNext(ResubmitRequest resubmitRequest) {
                    if (resubmitRequest.index < 0) { // special marker event
                        child.onCompleted();
                        resubmitRecords.remove(jobId);
                        return;
                    }
                    String workerId = getWorkerKey(resubmitRequest.stage, resubmitRequest.index);
                    final ResubmitRecord resubmitRecord = resubmitRecords.get(workerId);
                    long delay = evalDelay(resubmitRecord);
                    final ResubmitRecord result = new ResubmitRecord(workerId, scheduler.now() + delay * 1000, delay);
                    resubmitRecords.put(workerId, result);
                }
            };
        }

        private long evalDelay(ResubmitRecord resubmitRecord) {
            long delay = resubmitIntervalSecs[0];
            if (resubmitRecord != null) {
                long prevDelay = resubmitRecord.delayedBy;
                int index = 0;
                for (; index < resubmitIntervalSecs.length; index++) {
                    if (prevDelay <= resubmitIntervalSecs[index]) {
                        break;
                    }
                }
                index++;
                if (index >= resubmitIntervalSecs.length) {
                    index = resubmitIntervalSecs.length - 1;
                }
                delay = resubmitIntervalSecs[index];
            }
            return delay;
        }
    }

    private final MasterConfiguration config;
    private final Scheduler scheduler;

    private final Observer<ResubmitRequest> observer;
    private final ConcurrentMap<String, Map<String, ResubmitRecord>> resubmitRecordsMap;

    public DefaultWorkerResubmitRateLimiter(MasterConfiguration config, Scheduler scheduler) {
        this.config = config;
        this.scheduler = scheduler;
        resubmitRecordsMap = new ConcurrentHashMap<>();
        PublishSubject<ResubmitRequest> subject = PublishSubject.create();
        observer = new SerializedObserver<>(subject);
        subject
                .groupBy(resubmitRequest -> resubmitRequest.jobId)
                .flatMap(go -> go.lift(new ResubmitRateLimiterOperator<>(go.getKey())))
                .subscribe();
    }

    @Inject
    public DefaultWorkerResubmitRateLimiter(MasterConfiguration config) {
        this(config, Schedulers.computation());
    }

    // just key it with stage number, effectively rate limit based on stage instead of separately for each worker
    private String getWorkerKey(int stageNumber, int workerIndex) {
        return Integer.toString(stageNumber);
    }

    @Override
    public void delayWorkerResubmit(String jobId, int stage, int index) {
        observer.onNext(new ResubmitRequest(jobId, stage, index));
    }

    @Override
    public void endJob(String jobId) {
        observer.onNext(new ResubmitRequest(jobId, -1, -1));
    }

    @Override
    public long getResubmitAt(String jobId, int stageNumber, int workerIndex) {
        final Map<String, ResubmitRecord> resubmitRecords = resubmitRecordsMap.get(jobId);
        if (resubmitRecords == null) {
            return 0L;
        }
        final ResubmitRecord resubmitRecord = resubmitRecords.get(getWorkerKey(stageNumber, workerIndex));
        return resubmitRecord == null ? 0 : resubmitRecord.resubmitAt;
    }

}
