package io.netflix.titus.testkit.embedded.cloud.agent.player;

import java.util.concurrent.TimeUnit;

import com.netflix.titus.simulator.TitusCloudSimulator.SimulatedTaskStatus.SimulatedTaskState;
import io.netflix.titus.testkit.embedded.cloud.agent.TaskExecutorHolder;
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
        if (rule.getReasonCode().isPresent()) {
            taskHolder.transitionTo(
                    Protos.TaskState.TASK_FAILED,
                    Protos.TaskStatus.Reason.valueOf(rule.getReasonCode().get()),
                    rule.getReasonMessage().orElse("Reason details not provided")
            );
            return;
        }
        moveToNextState(simulatedState);
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
        }
        logger.warn("Unknown task state: {}", state);
        return SimulatedTaskState.Finished;
    }

    public boolean isTerminated() {
        return TaskExecutorHolder.isTerminal(taskHolder.getState());
    }
}
