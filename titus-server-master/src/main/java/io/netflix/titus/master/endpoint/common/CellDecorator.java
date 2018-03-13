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

package io.netflix.titus.master.endpoint.common;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import com.netflix.titus.grpc.protogen.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.JobModel;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;

public class CellDecorator {

    private final Supplier<String> cellNameSupplier;

    public CellDecorator(Supplier<String> cellNameSupplier) {
        this.cellNameSupplier = cellNameSupplier;
    }

    /**
     * Adds {@link JobModel#CELL_NAME_KEY "titus.cell"} and {@link JobModel#STACK_NAME_KEY "titus.stack"}
     * labels to a {@link TitusJobSpec V2 job spec} if they are not already present.
     */
    public TitusJobSpec ensureCellInfo(TitusJobSpec jobSpec) {
        final Map<String, String> originalLabels = jobSpec.getLabels();
        if (CollectionsExt.containsKeys(originalLabels, JobModel.CELL_NAME_KEY, JobModel.STACK_NAME_KEY)) {
            return jobSpec;
        }

        final Map<String, String> labels = new HashMap<>(originalLabels);
        final String cellName = cellNameSupplier.get();
        labels.putIfAbsent(JobModel.CELL_NAME_KEY, cellName);
        labels.putIfAbsent(JobModel.STACK_NAME_KEY, cellName);
        return new TitusJobSpec.Builder(jobSpec).labels(labels).build();
    }

    /**
     * Adds {@link JobModel#CELL_NAME_KEY "titus.cell"} and {@link JobModel#STACK_NAME_KEY "titus.stack"}
     * attributes to a {@link JobDescriptor V3 job spec} if they are not already present.
     */
    public JobDescriptor ensureCellInfo(JobDescriptor jobDescriptor) {
        final String cellName = cellNameSupplier.get();
        final JobDescriptor.Builder builder = jobDescriptor.toBuilder();
        if (!jobDescriptor.containsAttributes(JobModel.CELL_NAME_KEY)) {
            builder.putAttributes(JobModel.CELL_NAME_KEY, cellName);
        }
        if (!jobDescriptor.containsAttributes(JobModel.STACK_NAME_KEY)) {
            builder.putAttributes(JobModel.STACK_NAME_KEY, cellName);
        }
        return builder.build();
    }
}
