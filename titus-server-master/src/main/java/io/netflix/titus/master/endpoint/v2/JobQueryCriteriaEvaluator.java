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

package io.netflix.titus.master.endpoint.v2;

import java.util.List;
import java.util.Map;
import java.util.Set;

import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobType;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskState;
import io.netflix.titus.api.model.v2.parameter.Parameter;
import io.netflix.titus.api.model.v2.parameter.Parameters;
import io.netflix.titus.api.store.v2.V2JobMetadata;
import io.netflix.titus.runtime.endpoint.common.QueryUtils;
import io.netflix.titus.runtime.endpoint.JobQueryCriteria;

class JobQueryCriteriaEvaluator {
    static boolean matches(V2JobMetadata job, JobQueryCriteria<TitusTaskState, TitusJobType> criteria) {
        List<Parameter> parameters = job.getParameters();

        if (criteria.getAppName().isPresent()) {
            if (!criteria.getAppName().get().equals(Parameters.getAppName(parameters))) {
                return false;
            }
        }

        if (criteria.getImageName().isPresent()) {
            if (!criteria.getImageName().get().equals(Parameters.getImageName(parameters))) {
                return false;
            }
        }

        if (criteria.getJobType().isPresent()) {
            Parameters.JobType jobType = Parameters.getJobType(parameters);
            if (jobType == null) {
                return false;
            }
            TitusJobType v2Type = getType(jobType);
            if (v2Type != criteria.getJobType().get()) {
                return false;
            }
        }

        if (criteria.getJobGroupStack().isPresent()) {
            if (!criteria.getJobGroupStack().get().equals(Parameters.getJobGroupStack(parameters))) {
                return false;
            }
        }
        if (criteria.getJobGroupDetail().isPresent()) {
            if (!criteria.getJobGroupDetail().get().equals(Parameters.getJobGroupDetail(parameters))) {
                return false;
            }
        }
        if (criteria.getJobGroupSequence().isPresent()) {
            if (!criteria.getJobGroupSequence().get().equals(Parameters.getJobGroupSeq(parameters))) {
                return false;
            }
        }

        Map<String, Set<String>> expectedLabels = criteria.getLabels();
        if (!expectedLabels.isEmpty()) {
            Map<String, String> labels = Parameters.getLabels(parameters);
            if (labels == null || labels.isEmpty()) {
                return false;
            }
            boolean andOp = criteria.isLabelsAndOp();
            if (!QueryUtils.matchesAttributes(expectedLabels, labels, andOp)) {
                return false;
            }
        }
        return true;
    }

    private static TitusJobType getType(Parameters.JobType jobType) {
        switch (jobType) {
            case Batch:
                return TitusJobType.batch;
            case Service:
                return TitusJobType.service;
        }
        throw new IllegalArgumentException("Unexpected job type " + jobType);
    }
}
