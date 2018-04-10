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

package com.netflix.titus.testkit.embedded.cloud.agent.player;

import java.util.concurrent.TimeUnit;

import com.netflix.titus.simulator.TitusCloudSimulator.SimulatedTaskStatus.SimulatedTaskState;
import com.netflix.titus.testkit.embedded.cloud.agent.TaskExecutorHolder;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Scheduler;

class ContainerPlayer {

    private static final Logger logger = LoggerFactory.getLogger(ContainerPlayer.class);

    private static final long INTERVAL_MS = 1_000;

    private final TaskExecutorHolder taskHolder;
    private final ContainerRules rules;
    private final Scheduler.Worker worker;

    private SimulatedTaskState waitingInState;
    private long waitingUntilTimestamp;

    ContainerPlayer(TaskExecutorHolder taskHolder, ContainerRules rules, Scheduler scheduler) {
        this.taskHolder = taskHolder;
        this.rules = rules;
        this.worker = scheduler.createWorker();
        this.worker.schedulePeriodically(this::evaluate, 0L, INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    void shutdown() {
        worker.unsubscribe();
    }

    private void evaluate() {
        try {
            unsafeEvaluate();
        } catch (Exception e) {
            logger.warn("Error in evaluation process", e);
        }
    }

    private void unsafeEvaluate() {
        if (isTerminated()) {
            worker.unsubscribe();
        }
        Protos.TaskState state = taskHolder.getState();
        SimulatedTaskState simulatedState = toSimulatedTaskState(state);
        ContainerStateRule rule = rules.getTaskStateRules().get(simulatedState);
        if (rule == null) {
            moveToNextState(simulatedState);
        } else if (isWaitOver(simulatedState, rule)) {
            moveToNextState(simulatedState, rule);
        }
    }

    private boolean isWaitOver(SimulatedTaskState taskState, ContainerStateRule rule) {
        if (rule.getDelayInStateMs() == 0) {
            return true;
        }
        if (taskState == waitingInState) {
            return waitingUntilTimestamp <= worker.now();
        }
        waitingInState = taskState;
        waitingUntilTimestamp = worker.now() + rule.getDelayInStateMs();
        return false;
    }

    private void moveToNextState(SimulatedTaskState simulatedState, ContainerStateRule rule) {
        switch (rule.getAction()) {
            case None:
                moveToNextState(simulatedState);
                break;
            case Finish:
                moveToTerminalState(rule);
                break;
            case Forget:
                // TODO
                break;
        }
    }

    private void moveToTerminalState(ContainerStateRule rule) {
        Protos.TaskState mesosTaskState = rule.getMesosTerminalState().orElse(Protos.TaskState.TASK_ERROR);
        Protos.TaskStatus.Reason mesosReasonCode = rule.getMesosReasonCode().orElse(Protos.TaskStatus.Reason.REASON_COMMAND_EXECUTOR_FAILED);
        taskHolder.transitionTo(mesosTaskState, mesosReasonCode, rule.getReasonMessage().orElse("Reason details not provided"));
    }

    private void moveToNextState(SimulatedTaskState simulatedState) {
        switch (simulatedState) {
            case Launched:
                taskHolder.transitionTo(Protos.TaskState.TASK_STARTING);
                break;
            case StartInitiated:
                taskHolder.transitionTo(Protos.TaskState.TASK_RUNNING);
                break;
            case Started:
                taskHolder.transitionTo(Protos.TaskState.TASK_FINISHED);
                break;
            case KillInitiated:
                taskHolder.transitionTo(Protos.TaskState.TASK_KILLED);
        }
    }

    private SimulatedTaskState toSimulatedTaskState(Protos.TaskState state) {
        if (isTerminated()) {
            return SimulatedTaskState.Finished;
        }
        switch (state) {
            case TASK_STAGING:
                return SimulatedTaskState.Launched;
            case TASK_STARTING:
                return SimulatedTaskState.StartInitiated;
            case TASK_RUNNING:
                return SimulatedTaskState.Started;
            case TASK_KILLING:
                return SimulatedTaskState.KillInitiated;
        }
        logger.warn("Unknown task state: {}", state);
        return SimulatedTaskState.Finished;
    }

    public boolean isTerminated() {
        return TaskExecutorHolder.isTerminal(taskHolder.getState());
    }
}
