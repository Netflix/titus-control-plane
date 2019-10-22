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
package com.netflix.titus.gateway.service.v3.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.grpc.protogen.Page;
import com.netflix.titus.grpc.protogen.Pagination;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import com.netflix.titus.runtime.endpoint.common.EmptyLogStorageInfo;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Test;

import static com.netflix.titus.common.util.Evaluators.evaluateTimes;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class GatewayJobServiceGatewayTest {

    private static final int PAGE_SIZE = 2;

    private static final Page FIRST_PAGE = Page.newBuilder().setPageSize(PAGE_SIZE).build();

    private static final int JOB_SIZE = 6;

    private static final int ARCHIVED_TASKS_COUNT = JOB_SIZE * 2;

    private static final int ALL_TASKS_COUNT = JOB_SIZE + ARCHIVED_TASKS_COUNT;

    private static final Job<BatchJobExt> JOB = JobGenerator.batchJobsOfSize(JOB_SIZE).getValue();

    private static final List<Task> ARCHIVED_TASKS = evaluateTimes(ARCHIVED_TASKS_COUNT, i -> newGrpcTask(i, TaskState.Finished));

    private static final List<Task> ACTIVE_TASKS = evaluateTimes(JOB_SIZE, i -> newGrpcTask(ARCHIVED_TASKS_COUNT + i, TaskState.Started));

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

        List<Task> tasks = GatewayJobServiceGateway.deDupTasks(activeTasks, archivedTasks);
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

        tasks = GatewayJobServiceGateway.deDupTasks(activeTasks, archivedTasks);
        assertThat(tasks.size()).isEqualTo(4);
        taskIds = tasks.stream().map(Task::getId).collect(Collectors.toList());
        Collections.sort(taskIds);
        assertThat(taskIds).isEqualTo(expectedIdList);
    }

    @Test
    public void testCombineActiveAndArchiveTaskResultSetWithCursor() {
        testCombineActiveAndArchiveTaskResultSet(this::newTaskQueryWithCursor, this::takeActivePage);
    }

    @Test
    public void testCombineActiveAndArchiveTaskResultSetWithPageNumber() {
        testCombineActiveAndArchiveTaskResultSet(this::newTaskQueryWithPagNumber, p -> takeAllActive());
    }

    private void testCombineActiveAndArchiveTaskResultSet(Function<Pagination, TaskQuery> queryFunction, Function<Integer, TaskQueryResult> activePageResultFunction) {
        // First page
        TaskQueryResult combinedResult = testCombineForFirstPage();

        // Iterate using page numbers
        // Archive tasks first.
        Pagination lastPagination = combinedResult.getPagination();
        for (int p = 1; p < ARCHIVED_TASKS_COUNT / PAGE_SIZE; p++) {
            TaskQuery cursorQuery = queryFunction.apply(lastPagination);

            TaskQueryResult cursorResult = GatewayJobServiceGateway.combineTaskResults(cursorQuery, activePageResultFunction.apply(0), ARCHIVED_TASKS);
            checkCombinedResult(cursorResult, ARCHIVED_TASKS.subList(p * PAGE_SIZE, (p + 1) * PAGE_SIZE));

            lastPagination = cursorResult.getPagination();
        }

        // Now fetch the active data
        for (int p = 0; p < JOB_SIZE / PAGE_SIZE; p++) {
            TaskQuery cursorQuery = queryFunction.apply(lastPagination);

            TaskQueryResult cursorResult = GatewayJobServiceGateway.combineTaskResults(cursorQuery, activePageResultFunction.apply(p), ARCHIVED_TASKS);
            checkCombinedResult(cursorResult, ACTIVE_TASKS.subList(p * PAGE_SIZE, (p + 1) * PAGE_SIZE));

            lastPagination = cursorResult.getPagination();
        }
    }

    private TaskQueryResult testCombineForFirstPage() {
        TaskQuery taskQuery = TaskQuery.newBuilder().setPage(FIRST_PAGE).build();
        TaskQueryResult page0ActiveSetResult = takeActivePage(0);

        TaskQueryResult combinedResult = GatewayJobServiceGateway.combineTaskResults(taskQuery, page0ActiveSetResult, ARCHIVED_TASKS);
        checkCombinedResult(combinedResult, ARCHIVED_TASKS.subList(0, PAGE_SIZE));
        return combinedResult;
    }

    private TaskQuery newTaskQueryWithCursor(Pagination lastPagination) {
        return TaskQuery.newBuilder()
                .setPage(Page.newBuilder().setPageSize(PAGE_SIZE).setCursor(lastPagination.getCursor()))
                .build();
    }

    private TaskQuery newTaskQueryWithPagNumber(Pagination lastPagination) {
        return TaskQuery.newBuilder()
                .setPage(Page.newBuilder().setPageSize(PAGE_SIZE).setPageNumber(lastPagination.getCurrentPage().getPageNumber() + 1))
                .build();
    }

    private TaskQueryResult takeActivePage(int pageNumber) {
        return TaskQueryResult.newBuilder()
                .setPagination(Pagination.newBuilder()
                        .setCurrentPage(FIRST_PAGE.toBuilder().setPageNumber(pageNumber))
                        .setTotalItems(ACTIVE_TASKS.size())
                )
                .addAllItems(ACTIVE_TASKS.subList(pageNumber * PAGE_SIZE, (pageNumber + 1) * PAGE_SIZE))
                .build();
    }

    private TaskQueryResult takeAllActive() {
        return TaskQueryResult.newBuilder()
                .setPagination(Pagination.newBuilder()
                        .setCurrentPage(FIRST_PAGE)
                        .setTotalItems(ACTIVE_TASKS.size())
                )
                .addAllItems(ACTIVE_TASKS)
                .build();
    }

    private void checkCombinedResult(TaskQueryResult combinedResult, List<Task> expectedTaskList) {
        assertThat(combinedResult.getPagination().getCursor()).isNotNull();
        assertThat(combinedResult.getPagination().getTotalItems()).isEqualTo(ALL_TASKS_COUNT);
        assertThat(combinedResult.getPagination().getTotalPages()).isEqualTo(ALL_TASKS_COUNT / PAGE_SIZE);
        assertThat(expectedTaskList).isEqualTo(combinedResult.getItemsList());
    }

    private static Task newGrpcTask(int index, TaskState taskState) {
        BatchJobTask coreTask = JobGenerator.batchTasks(JOB).getValue().toBuilder()
                .withId("task#" + index)
                .withStatus(
                        com.netflix.titus.api.jobmanager.model.job.TaskStatus.newBuilder()
                                .withState(taskState)
                                .withTimestamp(index)
                                .build()
                ).build();
        return GrpcJobManagementModelConverters.toGrpcTask(coreTask, EmptyLogStorageInfo.empty());
    }
}
