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

package com.netflix.titus.testkit.junit.master;

import java.util.Iterator;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.netflix.titus.testkit.client.TitusMasterClient;
import com.netflix.titus.testkit.embedded.master.EmbeddedTitusMaster;
import com.netflix.titus.api.endpoint.v2.rest.representation.TaskInfo;
import com.netflix.titus.api.endpoint.v2.rest.representation.TitusJobInfo;
import com.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskInfo;
import com.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskState;
import com.netflix.titus.api.model.event.SchedulingEvent;
import com.netflix.titus.common.util.rx.eventbus.RxEventBus;
import com.netflix.titus.master.endpoint.v2.rest.representation.JobSetInstanceCountsCmd;
import com.netflix.titus.testkit.client.TitusMasterClient;
import com.netflix.titus.testkit.embedded.master.EmbeddedTitusMaster;
import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import static com.netflix.titus.common.util.CollectionsExt.asSet;
import static com.netflix.titus.common.util.CollectionsExt.isNullOrEmpty;

public class JobObserver {

    private static final Logger logger = LoggerFactory.getLogger(JobObserver.class);

    private static final String CLIENT_ID = "jobObserverClient";

    private static final int TIMEOUT = 10 * 1000;

    private final String jobId;
    private final TitusMasterClient client;
    private final RxEventBus eventBus;
    private final EmbeddedTitusMaster titusMaster;

    public JobObserver(String jobId, EmbeddedTitusMaster titusMaster) {
        this.jobId = jobId;
        this.client = titusMaster.getClient();
        this.eventBus = titusMaster.getEventBus();
        this.titusMaster = titusMaster;
    }

    public int awaitJobInState(TitusTaskState state) throws InterruptedException {
        TitusJobInfo jobInfo = client.findJob(jobId, false).toBlocking().first();
        String[] taskIds = jobInfo.getTasks().stream().map(TaskInfo::getId).toArray(String[]::new);
        if (taskIds.length > 0) {
            awaitTasksInState(state, taskIds);
        }
        return taskIds.length;
    }

    public void awaitTasksInState(TitusTaskState expectedTaskState, String... taskIds) throws InterruptedException {
        Preconditions.checkArgument(!isNullOrEmpty(taskIds));

        ExtTestSubscriber<SchedulingEvent> eventSubscriber = new ExtTestSubscriber<>();
        events().subscribe(eventSubscriber);

        long deadline = System.currentTimeMillis() + TIMEOUT;
        Set<String> remaining = asSet(taskIds);
        try {
            while (!remaining.isEmpty() && deadline > System.currentTimeMillis()) {
                final Iterator<String> remainingIterator = remaining.iterator();
                while (remainingIterator.hasNext()) {
                    String taskId = remainingIterator.next();
                    TitusTaskInfo taskInfo = client.findTask(taskId, true).toBlocking().first();
                    TitusTaskState checkedState = taskInfo.getState();
                    if (checkedState == expectedTaskState) {
                        remainingIterator.remove();
                    } else {
                        verifyTaskStateBeforeExpected(taskInfo, expectedTaskState);
                    }
                }
            }
        } finally {
            eventSubscriber.unsubscribe();
        }

        if (remaining.isEmpty()) {
            return;
        }

        remaining.forEach(t -> {
            logger.info("Gave up on waiting on task {} to reach state {}. Scheduling failure: {}", t, expectedTaskState, titusMaster.reportForTask(t));
        });
        throw new IllegalStateException(expectedTaskState + " state not reached by tasks " + remaining);
    }

    public void updateJobSize(int desired, int min, int max) throws InterruptedException {
        JobSetInstanceCountsCmd cmd = new JobSetInstanceCountsCmd(CLIENT_ID, jobId, desired, min, max);
        client.setInstanceCount(cmd).toBlocking().firstOrDefault(null);
        int runningCount = awaitJobInState(TitusTaskState.RUNNING);
        if (runningCount != desired) {
            throw new IllegalStateException("Expected " + desired + " to be running, but found " + runningCount);
        }
    }

    public void terminateAndShrink(String taskId) throws InterruptedException {
        client.killTaskAndShrink(taskId).toBlocking().firstOrDefault(null);
        awaitTasksInState(TitusTaskState.FAILED, taskId);
    }

    private Observable<SchedulingEvent> events() {
        return eventBus
                .listen("jobObserver#jobId", SchedulingEvent.class)
                .filter(event -> event.getJobId().equals(jobId));
    }

    private static void verifyTaskStateBeforeExpected(TitusTaskInfo taskInfo, TitusTaskState expectedTaskState) {
        TitusTaskState checkedState = taskInfo.getState();
        if (!TitusTaskState.isBefore(checkedState, expectedTaskState)) {
            throw new IllegalArgumentException("Task " + taskInfo.getId() +
                    " has state more advanced than expected: " + expectedTaskState + " < " + checkedState);
        }
    }
}
