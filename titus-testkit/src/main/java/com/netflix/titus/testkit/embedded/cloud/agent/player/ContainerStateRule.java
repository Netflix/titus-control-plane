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

import java.util.Objects;
import java.util.Optional;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.testkit.embedded.cloud.agent.TaskExecutorHolder;
import org.apache.mesos.Protos;

class ContainerStateRule {

    enum Action {
        None,
        Finish,
        Forget
    }

    private final long delayInStateMs;
    private final Action action;
    private final Optional<Protos.TaskState> mesosTerminalState;
    private final Optional<Protos.TaskStatus.Reason> mesosReasonCode;
    private final Optional<String> reasonMessage;

    ContainerStateRule(long delayInStateMs,
                       Action action,
                       Optional<Protos.TaskState> mesosTerminalState,
                       Optional<Protos.TaskStatus.Reason> mesosReasonCode,
                       Optional<String> reasonMessage) {
        this.delayInStateMs = delayInStateMs;
        this.action = action;
        this.mesosTerminalState = mesosTerminalState;
        this.mesosReasonCode = mesosReasonCode;
        this.reasonMessage = reasonMessage;
    }

    public long getDelayInStateMs() {
        return delayInStateMs;
    }

    public Action getAction() {
        return action;
    }

    public Optional<Protos.TaskState> getMesosTerminalState() {
        return mesosTerminalState;
    }

    public Optional<Protos.TaskStatus.Reason> getMesosReasonCode() {
        return mesosReasonCode;
    }

    public Optional<String> getReasonMessage() {
        return reasonMessage;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ContainerStateRule that = (ContainerStateRule) o;
        return delayInStateMs == that.delayInStateMs &&
                action == that.action &&
                Objects.equals(mesosTerminalState, that.mesosTerminalState) &&
                Objects.equals(mesosReasonCode, that.mesosReasonCode) &&
                Objects.equals(reasonMessage, that.reasonMessage);
    }

    @Override
    public int hashCode() {

        return Objects.hash(delayInStateMs, action, mesosTerminalState, mesosReasonCode, reasonMessage);
    }

    @Override
    public String toString() {
        return "ContainerStateRule{" +
                "delayInStateMs=" + delayInStateMs +
                ", action=" + action +
                ", mesosTerminalState=" + mesosTerminalState +
                ", mesosReasonCode=" + mesosReasonCode +
                ", reasonMessage=" + reasonMessage +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {

        private long delayInStateMs;
        private Action action = Action.None;
        private Protos.TaskState mesosTerminalState;
        private Protos.TaskStatus.Reason mesosReasonCode;
        private String titusReasonCode;
        private String reasonMessage;

        private Builder() {
        }

        public Builder withDelayInStateMs(long delayInStateMs) {
            this.delayInStateMs = delayInStateMs;
            return this;
        }

        public Builder withAction(Action action) {
            this.action = action;
            return this;
        }

        public Builder withMesosTerminalState(Protos.TaskState mesosTerminalState) {
            Preconditions.checkArgument(TaskExecutorHolder.isTerminal(mesosTerminalState), "Not a terminal state: %s", mesosTerminalState);
            this.action = Action.Finish;
            this.mesosTerminalState = mesosTerminalState;
            return this;
        }

        public Builder withMesosReasonCode(Protos.TaskStatus.Reason mesosReasonCode) {
            this.mesosReasonCode = mesosReasonCode;
            return this;
        }

        public Builder withTitusReasonCode(String titusReasonCode) {
            this.titusReasonCode = titusReasonCode;
            return this;
        }

        public Builder withReasonMessage(String reasonMessage) {
            this.reasonMessage = reasonMessage;
            return this;
        }

        public ContainerStateRule build() {
            Protos.TaskState effectiveMesosTerminalState = mesosTerminalState;
            Protos.TaskStatus.Reason effectiveMesosReasonCode = mesosReasonCode;
            String effectiveReasonMessage = reasonMessage;

            if (mesosReasonCode == null && titusReasonCode != null) {
                switch (titusReasonCode) {
                    case TaskStatus.REASON_INVALID_REQUEST:
                    case TaskStatus.REASON_CRASHED:
                    case TaskStatus.REASON_FAILED:
                    case TaskStatus.REASON_LOCAL_SYSTEM_ERROR:
                    case TaskStatus.REASON_TRANSIENT_SYSTEM_ERROR:
                    case TaskStatus.REASON_UNKNOWN_SYSTEM_ERROR:
                        effectiveMesosTerminalState = mesosTerminalState == null ? Protos.TaskState.TASK_FAILED : mesosTerminalState;
                        effectiveMesosReasonCode = Protos.TaskStatus.Reason.REASON_COMMAND_EXECUTOR_FAILED;
                        break;
                    case TaskStatus.REASON_TASK_KILLED:
                        effectiveMesosTerminalState = mesosTerminalState == null ? Protos.TaskState.TASK_KILLED : mesosTerminalState;
                        effectiveMesosReasonCode = Protos.TaskStatus.Reason.REASON_TASK_KILLED_DURING_LAUNCH;
                        break;
                    case TaskStatus.REASON_TASK_LOST:
                        effectiveMesosTerminalState = mesosTerminalState == null ? Protos.TaskState.TASK_LOST : mesosTerminalState;
                        effectiveMesosReasonCode = Protos.TaskStatus.Reason.REASON_TASK_UNKNOWN;
                        break;
                    default:
                        effectiveMesosTerminalState = mesosTerminalState == null ? Protos.TaskState.TASK_UNKNOWN : mesosTerminalState;
                        effectiveMesosReasonCode = Protos.TaskStatus.Reason.REASON_TASK_INVALID;
                }
                effectiveReasonMessage = String.format("%s: (from titusReasonCode=%s)", StringExt.safeTrim(reasonMessage), titusReasonCode);
            }

            return new ContainerStateRule(delayInStateMs, action, Optional.ofNullable(effectiveMesosTerminalState),
                    Optional.ofNullable(effectiveMesosReasonCode), Optional.ofNullable(effectiveReasonMessage)
            );
        }
    }
}
