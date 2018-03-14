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
package io.netflix.titus.gateway.service.v3.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.netflix.titus.grpc.protogen.Task;
import org.junit.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class DefaultJobManagementServiceTest {

    @Test
    public void verifyDeDupTaskIds() {
        List<String> expectedIdList = new ArrayList<String>() {{
            add("task1");
            add("task2");
            add("task3");
            add("task4");
        }};

        // with duplicate ids
        List<Task> activeTasks = new ArrayList<>();
        activeTasks.add(Task.newBuilder().setId("task1").build());
        activeTasks.add(Task.newBuilder().setId("task2").build());
        activeTasks.add(Task.newBuilder().setId("task3").build());

        List<Task> archivedTasks = new ArrayList<>();
        archivedTasks.add(Task.newBuilder().setId("task2").build());
        archivedTasks.add(Task.newBuilder().setId("task3").build());
        archivedTasks.add(Task.newBuilder().setId("task4").build());

        List<Task> tasks = DefaultJobManagementService.deDupTasks(activeTasks, archivedTasks);
        assertThat(tasks.size()).isEqualTo(4);

        List<String> taskIds = tasks.stream().map(Task::getId).sorted().collect(Collectors.toList());
        assertThat(taskIds).isEqualTo(expectedIdList);

        // disjoint lists
        activeTasks = new ArrayList<>();
        activeTasks.add(Task.newBuilder().setId("task1").build());
        activeTasks.add(Task.newBuilder().setId("task2").build());

        archivedTasks = new ArrayList<>();
        archivedTasks.add(Task.newBuilder().setId("task3").build());
        archivedTasks.add(Task.newBuilder().setId("task4").build());

        tasks = DefaultJobManagementService.deDupTasks(activeTasks, archivedTasks);
        assertThat(tasks.size()).isEqualTo(4);
        taskIds = tasks.stream().map(Task::getId).collect(Collectors.toList());
        Collections.sort(taskIds);
        assertThat(taskIds).isEqualTo(expectedIdList);

    }

}
