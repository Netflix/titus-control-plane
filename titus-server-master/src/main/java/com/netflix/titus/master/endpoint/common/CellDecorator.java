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

package com.netflix.titus.master.endpoint.common;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;

public class CellDecorator {

    private final Supplier<String> cellNameSupplier;

    public CellDecorator(Supplier<String> cellNameSupplier) {
        this.cellNameSupplier = cellNameSupplier;
    }

    /**
     * Adds a {@link JobAttributes#JOB_ATTRIBUTES_CELL "titus.cell"} label to a {@link TitusJobSpec V2 job spec},
     * replacing the existing value if it is already present.
     */
    public TitusJobSpec ensureCellInfo(TitusJobSpec jobSpec) {
        final Map<String, String> originalLabels = jobSpec.getLabels();
        final Map<String, String> labels = originalLabels == null ? new HashMap<>() : new HashMap<>(originalLabels);
        final String cellName = cellNameSupplier.get();
        labels.put(JobAttributes.JOB_ATTRIBUTES_CELL, cellName);
        return new TitusJobSpec.Builder(jobSpec).labels(labels).build();
    }

    /**
     * Adds a {@link JobAttributes#JOB_ATTRIBUTES_CELL "titus.cell"} attribute to a {@link JobDescriptor V3 job spec},
     * replacing the existing value if it is already present.
     */
    public JobDescriptor ensureCellInfo(JobDescriptor jobDescriptor) {
        final String cellName = cellNameSupplier.get();
        return jobDescriptor.toBuilder()
                .putAttributes(JobAttributes.JOB_ATTRIBUTES_CELL, cellName)
                .build();
    }
}
