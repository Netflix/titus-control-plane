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

package io.netflix.titus.master.integration.v3.scenario;

import com.netflix.titus.grpc.protogen.TaskStatus;
import org.apache.mesos.Protos;
import org.junit.Test;

import static java.util.Arrays.stream;

/**
 */
final class ScenarioBuilderUtil {

    static long TIMEOUT_MS = 30_000;

    static String discoverActiveTest() {
        return stream(Thread.currentThread().getStackTrace())
                .filter(seg -> {
                    try {
                        return Class.forName(seg.getClassName()).getMethod(seg.getMethodName()).getAnnotation(Test.class) != null;
                    } catch (Exception ignore) {
                        return false;
                    }
                })
                .findFirst()
                .map(StackTraceElement::getMethodName)
                .orElse("?");
    }

    static Protos.TaskState toMesosTaskState(TaskStatus.TaskState taskState) {
        switch (taskState) {
            case Launched:
                return Protos.TaskState.TASK_STAGING;
            case StartInitiated:
                return Protos.TaskState.TASK_STARTING;
            case Started:
                return Protos.TaskState.TASK_RUNNING;
            case Finished:
                return Protos.TaskState.TASK_FINISHED;
        }
        throw new IllegalArgumentException(taskState + " is not present in Mesos protocol");
    }

    static TaskStatus.TaskState fromMesosTaskState(Protos.TaskState mesosTaskState) {
        switch (mesosTaskState) {
            case TASK_STAGING:
                return TaskStatus.TaskState.Launched;
            case TASK_STARTING:
                return TaskStatus.TaskState.StartInitiated;
            case TASK_RUNNING:
                return TaskStatus.TaskState.Started;
            case TASK_KILLING:
                return TaskStatus.TaskState.KillInitiated;
            case TASK_FINISHED:
            case TASK_FAILED:
            case TASK_ERROR:
            case TASK_LOST:
            case TASK_KILLED:
                return TaskStatus.TaskState.Finished;
        }
        throw new IllegalArgumentException("Unknown Mesos task state found: " + mesosTaskState);
    }
}
