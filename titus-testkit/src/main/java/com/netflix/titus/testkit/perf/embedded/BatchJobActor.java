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

package com.netflix.titus.testkit.perf.embedded;

import java.util.concurrent.TimeUnit;

import com.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;
import com.netflix.titus.testkit.client.TitusMasterClient;
import com.netflix.titus.testkit.embedded.cloud.agent.TaskExecutorHolder;
import com.netflix.titus.testkit.embedded.master.EmbeddedTitusMaster;
import com.netflix.titus.testkit.junit.master.TitusMasterResource;
import org.apache.mesos.Protos;
import rx.Observable;
import rx.Subscription;
import rx.subjects.ReplaySubject;

/**
 * Encapsulates a logic of running a single batch job within {@link TitusMasterResource}.
 */
class BatchJobActor {

    private final TitusJobSpec jobSpec;
    private final EmbeddedTitusMaster titusMaster;
    private final TitusMasterClient client;
    private final long terminateAt;

    BatchJobActor(TitusJobSpec jobSpec, int jobDurationSec, EmbeddedTitusMaster titusMaster) {
        this.jobSpec = jobSpec;
        this.client = titusMaster.getClient();
        this.titusMaster = titusMaster;
        this.terminateAt = System.currentTimeMillis() + jobDurationSec * 1000;
    }

    /**
     * Execute a job, wait for its termination, emit job id and complete.
     */
    Observable<String> execute() {
        return submitJob().flatMap(this::waitForTermination).map(StateValueHolder::getState);
    }

    /**
     * Return observable that on subscriber submits a job, emits first task of this job, and completes.
     * The job is also moved to RUNNING state.
     */
    private Observable<StateValueHolder<String, TaskExecutorHolder>> submitJob() {
        return Observable.create(subscriber -> {
            ReplaySubject<TaskExecutorHolder> taskHolders = ReplaySubject.create();
            Subscription taskHoldersSubscription = titusMaster.observeLaunchedTasks().subscribe(taskHolders);

            client.submitJob(jobSpec)
                    .flatMap(jobId -> taskHolders
                            .filter(th -> th.getJobId().equals(jobId))
                            .map(th -> new StateValueHolder<>(jobId, th))
                    )
                    .doOnTerminate(taskHoldersSubscription::unsubscribe)
                    .doOnNext(stateValueHolder -> {
                        TaskExecutorHolder taskHolder = stateValueHolder.getValue();
                        taskHolder.transitionTo(Protos.TaskState.TASK_STARTING);
                        taskHolder.transitionTo(Protos.TaskState.TASK_RUNNING);
                    })
                    .subscribe(subscriber);
        });
    }

    private Observable<StateValueHolder<String, TaskExecutorHolder>> waitForTermination(
            StateValueHolder<String, TaskExecutorHolder> jobIdTaskHolderPair) {

        Observable terminationObservable = Observable.create(subscriber -> {
            long delay = terminateAt - System.currentTimeMillis();
            String jobId = jobIdTaskHolderPair.getState();
            if (delay <= 0) {
                client.killJob(jobId).subscribe();
            } else {
                Observable.timer(delay, TimeUnit.MILLISECONDS).flatMap(tick -> client.killJob(jobId)).subscribe(subscriber);
            }
        });

        return terminationObservable.concatWith(Observable.just(jobIdTaskHolderPair));
    }

    private static class StateValueHolder<S, V> {
        private final S state;
        private final V value;

        StateValueHolder(S state, V value) {
            this.state = state;
            this.value = value;
        }

        S getState() {
            return state;
        }

        V getValue() {
            return value;
        }
    }
}
