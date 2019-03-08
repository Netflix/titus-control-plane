/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.master.jobactivity.store;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.api.jobactivity.store.JobActivityPublisherRecord;
import com.netflix.titus.api.jobactivity.store.JobActivityPublisherStore;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.runtime.endpoint.common.EmptyLogStorageInfo;
import com.netflix.titus.runtime.jobactivity.JobActivityPublisherRecordUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * This implementation of a {@link JobActivityPublisherStore} is an in-memory
 * implementation intended for testing.
 */
@Singleton
public class InMemoryJobActivityPublisherStore implements JobActivityPublisherStore {
    private static final Logger logger = LoggerFactory.getLogger(InMemoryJobActivityPublisherStore.class);

    private final List<Pair<Optional<Job<?>>, Optional<Task>>> taskAndJobList;

    @Inject
    public InMemoryJobActivityPublisherStore() {
        taskAndJobList = new LinkedList<>();
    }

    @Override
    public Mono<Void> publishJob(Job<?> job) {
        taskAndJobList.add(new Pair<>(Optional.of(job), Optional.empty()));
        return Mono.empty();
    }

    @Override
    public Mono<Void> publishTask(Task task) {
        taskAndJobList.add(new Pair<>(Optional.empty(), Optional.of(task)));
        return Mono.empty();
    }

    @VisibleForTesting
    public Flux<JobActivityPublisherRecord> getRecords() {
        return Flux.fromIterable(taskAndJobList.stream()
                .map(taskAndJobPair -> {
                    if (taskAndJobPair.getLeft().isPresent()) {
                        return new JobActivityPublisherRecord(0,
                                (short)JobActivityPublisherRecord.RecordType.JOB.ordinal(),
                                JobActivityPublisherRecordUtils.jobToByteArry(taskAndJobPair.getLeft().get()));
                    } else if (taskAndJobPair.getRight().isPresent()) {
                        return new JobActivityPublisherRecord(0,
                                (short)JobActivityPublisherRecord.RecordType.TASK.ordinal(),
                                JobActivityPublisherRecordUtils.taskToByteArray(taskAndJobPair.getRight().get(), EmptyLogStorageInfo.empty()));
                    }
                    return null;
                })
                .collect(Collectors.toList())
        );
    }

    @VisibleForTesting
    public Mono<Integer> getSize() {
        return Mono.just(taskAndJobList.size());
    }
}
