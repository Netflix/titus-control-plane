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

package com.netflix.titus.master.mesos.kubeapiserver;

import java.time.Duration;
import java.util.Map;

import io.titanframework.messages.TitanProtos;
import org.apache.mesos.Protos;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class PerformanceToolUtilTest {

    @Test
    public void testParser() {
        Protos.TaskInfo taskInfo = newTaskInfo(
                "TASK_LIFECYCLE_1",
                "selector: slots=0.. slotStep=2; launched: delay=2s; startInitiated: delay=3s; started: delay=60s; killInitiated: delay=5s"
        );
        Map<String, String> annotations = PerformanceToolUtil.findPerformanceTestAnnotations(taskInfo);
        assertThat(annotations).containsEntry(PerformanceToolUtil.PREPARE_TIME, "3s");
        assertThat(annotations).containsEntry(PerformanceToolUtil.RUN_TIME, "60s");
        assertThat(annotations).containsEntry(PerformanceToolUtil.KILL_TIME, "5s");
    }

    private Protos.TaskInfo newTaskInfo(String key, String value) {
        return Protos.TaskInfo.newBuilder()
                .setTaskId(Protos.TaskID.newBuilder().setValue("myTask"))
                .setName("myTask")
                .setSlaveId(Protos.SlaveID.newBuilder().setValue("slave").build())
                .setData(TitanProtos.ContainerInfo.newBuilder()
                        .putUserProvidedEnv(key, value)
                        .setNetworkConfigInfo(TitanProtos.ContainerInfo.NetworkConfigInfo.newBuilder()
                                .setEniLabel("eni0")
                                .setEniLablel("eni0")
                                .addSecurityGroups("sg-123456")
                        )
                        .putTitusProvidedEnv("TITUS_JOB_ID", "testJob")
                        .build()
                        .toByteString()
                )
                .build();
    }
}