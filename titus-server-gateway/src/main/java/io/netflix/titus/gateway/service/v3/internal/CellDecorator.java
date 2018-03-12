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

package io.netflix.titus.gateway.service.v3.internal;

import java.util.function.Supplier;

import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobQueryResult;

class CellDecorator {
    /**
     * Stack name that can be replaced in a federated deployment, where all Cells have the same Stack name.
     */
    private static final String STACK_NAME_KEY = "titus.stack";
    /**
     * Unique Cell name for a deployment.
     */
    private static final String CELL_NAME_KEY = "titus.cell";

    private final Supplier<String> cellNameSupplier;

    CellDecorator(Supplier<String> cellNameSupplier) {
        this.cellNameSupplier = cellNameSupplier;
    }

    Job addCellInfo(Job job) {
        final String name = cellNameSupplier.get();
        final JobDescriptor jobDescriptor = job.getJobDescriptor().toBuilder()
                .putAttributes(CELL_NAME_KEY, name)
                .putAttributes(STACK_NAME_KEY, name)
                .build();
        return job.toBuilder().setJobDescriptor(jobDescriptor).build();
    }

    JobChangeNotification addCellInfo(JobChangeNotification notification) {
        switch (notification.getNotificationCase()) {
            case JOBUPDATE:
                JobChangeNotification.JobUpdate jobUpdate = notification.getJobUpdate().toBuilder()
                        .setJob(addCellInfo(notification.getJobUpdate().getJob()))
                        .build();
                return notification.toBuilder().setJobUpdate(jobUpdate).build();
            case TASKUPDATE:
                // TODO(fabio): decorate tasks
                return notification;
            default:
                return notification;
        }
    }

    JobQueryResult addCellInfo(JobQueryResult result) {
        JobQueryResult.Builder builder = result.toBuilder();
        for (int i = 0; i < result.getItemsCount(); i++) {
            builder.setItems(i, addCellInfo(result.getItems(i)));
        }
        return builder.build();
    }
}
