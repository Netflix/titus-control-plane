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
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;

public class CellDecorator {
    /**
     * Stack name that can be replaced in a federated deployment, where all Cells have the same Stack name.
     */
    public static final String STACK_NAME_KEY = "titus.stack";
    /**
     * Unique Cell name for a deployment.
     */
    public static final String CELL_NAME_KEY = "titus.cell";

    private final Supplier<String> cellNameSupplier;

    public CellDecorator(Supplier<String> cellNameSupplier) {
        this.cellNameSupplier = cellNameSupplier;
    }

    /**
     * Adds {@link CellDecorator#CELL_NAME_KEY "titus.cell"} and {@link CellDecorator#STACK_NAME_KEY "titus.stack"}
     * labels to a {@link TitusJobSpec V2 job spec} if they are not already present.
     */
    public TitusJobSpec ensureCellInfo(TitusJobSpec jobSpec) {
        final Map<String, String> originalLabels = jobSpec.getLabels();
        if (CollectionsExt.containsKeys(originalLabels, CELL_NAME_KEY, STACK_NAME_KEY)) {
            return jobSpec;
        }

        final Map<String, String> labels = new HashMap<>(originalLabels);
        final String cellName = cellNameSupplier.get();
        labels.putIfAbsent(CELL_NAME_KEY, cellName);
        labels.putIfAbsent(STACK_NAME_KEY, cellName);
        return new TitusJobSpec.Builder(jobSpec).labels(labels).build();
    }

    /**
     * Adds {@link CellDecorator#CELL_NAME_KEY "titus.cell"} and {@link CellDecorator#STACK_NAME_KEY "titus.stack"}
     * attributes to a {@link JobDescriptor V3 job spec} if they are not already present.
     */
    public JobDescriptor ensureCellInfo(JobDescriptor jobDescriptor) {
        final String cellName = cellNameSupplier.get();
        final JobDescriptor.Builder builder = jobDescriptor.toBuilder();
        if (!jobDescriptor.containsAttributes(CELL_NAME_KEY)) {
            builder.putAttributes(CELL_NAME_KEY, cellName);
        }
        if (!jobDescriptor.containsAttributes(STACK_NAME_KEY)) {
            builder.putAttributes(STACK_NAME_KEY, cellName);
        }
        return builder.build();
    }
}
