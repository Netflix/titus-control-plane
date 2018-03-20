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

package io.netflix.titus.master.integration.v3.job;

import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.Task;
import io.netflix.titus.testkit.model.job.JobDescriptorGenerator;

import static org.assertj.core.api.Assertions.assertThat;

public class CellAssertions {
    public static void assertCellInfo(JobDescriptor jobDescriptor) {
        assertCellInfo(jobDescriptor, JobDescriptorGenerator.TEST_CELL_NAME);
    }

    public static void assertCellInfo(JobDescriptor jobDescriptor, String cellName) {
        assertThat(jobDescriptor.getAttributesMap()).containsEntry("titus.cell", cellName);
        assertThat(jobDescriptor.getAttributesMap()).containsEntry("titus.stack", cellName);
    }

    public static void assertCellInfo(Task task, String cellName) {
        assertThat(task.getTaskContextMap()).containsEntry("titus.cell", cellName);
        assertThat(task.getTaskContextMap()).containsEntry("titus.stack", cellName);
    }
}
