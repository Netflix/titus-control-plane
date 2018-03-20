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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobType;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskState;
import io.netflix.titus.api.model.v2.parameter.Parameters;
import io.netflix.titus.api.model.v2.parameter.Parameters.JobType;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.master.store.V2JobMetadataWritable;
import io.netflix.titus.runtime.endpoint.JobQueryCriteria;
import io.netflix.titus.testkit.model.runtime.RuntimeModelGenerator;
import org.junit.Before;
import org.junit.Test;

import static io.netflix.titus.api.jobmanager.JobAttributes.JOB_ATTRIBUTES_CELL;
import static io.netflix.titus.api.model.v2.parameter.Parameters.mergeParameters;
import static io.netflix.titus.api.model.v2.parameter.Parameters.updateParameter;
import static io.netflix.titus.common.util.CollectionsExt.asMap;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class JobQueryCriteriaEvaluatorTest {

    private final String cellName = UUID.randomUUID().toString();
    private final RuntimeModelGenerator runtimeModel = new RuntimeModelGenerator(cellName);

    private final V2JobMetadataWritable job = (V2JobMetadataWritable) runtimeModel.newJobMetadata(JobType.Batch, "myJob");

    private final JobQueryCriteria.Builder<TitusTaskState, TitusJobType> queryBuilder = JobQueryCriteria.newBuilder();

    @Before
    public void setUp() throws Exception {
        Map<String, String> myLabels = CollectionsExt.merge(
                Parameters.getLabels(job.getParameters()),
                asMap("labelA", "valueA", "labelB", "valueB")
        );
        job.setParameters(mergeParameters(
                job.getParameters(),
                asList(
                        Parameters.newJobGroupStack("myStack"),
                        Parameters.newJobGroupDetail("myDetail"),
                        Parameters.newJobGroupSequence("mySequence"),
                        Parameters.newLabelsParameter(myLabels)
                )));
    }

    @Test
    public void testAppNameCriteria() throws Exception {
        // Matching
        JobQueryCriteria<TitusTaskState, TitusJobType> query = queryBuilder.withAppName("myJob").build();
        assertThat(JobQueryCriteriaEvaluator.matches(job, query)).isTrue();

        // Not matching
        JobQueryCriteria<TitusTaskState, TitusJobType> query2 = queryBuilder.withAppName("missing_job").build();
        assertThat(JobQueryCriteriaEvaluator.matches(job, query2)).isFalse();
    }

    @Test
    public void testImageNameCriteria() throws Exception {
        job.setParameters(updateParameter(job.getParameters(), Parameters.newImageNameParameter("myImage")));

        // Matching
        JobQueryCriteria<TitusTaskState, TitusJobType> query = queryBuilder.withImageName("myImage").build();
        assertThat(JobQueryCriteriaEvaluator.matches(job, query)).isTrue();

        // Not matching
        JobQueryCriteria<TitusTaskState, TitusJobType> query2 = queryBuilder.withImageName("missing_image").build();
        assertThat(JobQueryCriteriaEvaluator.matches(job, query2)).isFalse();
    }

    @Test
    public void testJobTypeCriteria() throws Exception {
        // Matching
        JobQueryCriteria<TitusTaskState, TitusJobType> query = queryBuilder.withJobType(TitusJobType.batch).build();
        assertThat(JobQueryCriteriaEvaluator.matches(job, query)).isTrue();

        // Not matching
        JobQueryCriteria<TitusTaskState, TitusJobType> query2 = queryBuilder.withJobType(TitusJobType.service).build();
        assertThat(JobQueryCriteriaEvaluator.matches(job, query2)).isFalse();
    }

    @Test
    public void testSingleLabelCriteria() throws Exception {
        // Matching label name only
        JobQueryCriteria<TitusTaskState, TitusJobType> query = queryBuilder.withLabels(expectedLabels().with("labelA").build()).build();
        assertThat(JobQueryCriteriaEvaluator.matches(job, query)).isTrue();

        // No label name match
        JobQueryCriteria<TitusTaskState, TitusJobType> query2 = queryBuilder.withLabels(expectedLabels().with("missing_label").build()).build();
        assertThat(JobQueryCriteriaEvaluator.matches(job, query2)).isFalse();

        // Matching label and value
        JobQueryCriteria<TitusTaskState, TitusJobType> query3 = queryBuilder.withLabels(expectedLabels().with("labelA", "valueA").build()).build();
        assertThat(JobQueryCriteriaEvaluator.matches(job, query3)).isTrue();

        // Not matching value
        JobQueryCriteria<TitusTaskState, TitusJobType> query4 = queryBuilder.withLabels(expectedLabels().with("labelA", "wrong_value").build()).build();
        assertThat(JobQueryCriteriaEvaluator.matches(job, query4)).isFalse();

        // Cell name
        JobQueryCriteria<TitusTaskState, TitusJobType> query5 = queryBuilder.withLabels(expectedLabels()
                .with(JOB_ATTRIBUTES_CELL).build())
                .build();
        assertThat(JobQueryCriteriaEvaluator.matches(job, query5)).isTrue();
        JobQueryCriteria<TitusTaskState, TitusJobType> query6 = queryBuilder.withLabels(expectedLabels()
                .with(JOB_ATTRIBUTES_CELL, cellName).build())
                .build();
        assertThat(JobQueryCriteriaEvaluator.matches(job, query6)).isTrue();
    }

    @Test
    public void testMultipleLabelsWithAndOperatorCriteria() throws Exception {
        // Two labels that are present
        JobQueryCriteria<TitusTaskState, TitusJobType> query = queryBuilder.withLabels(
                expectedLabels().with("labelA").with("labelB").build()
        ).withLabelsAndOp(true).build();

        assertThat(JobQueryCriteriaEvaluator.matches(job, query)).isTrue();

        // One of the labels is missing
        JobQueryCriteria<TitusTaskState, TitusJobType> query2 = queryBuilder.withLabels(
                expectedLabels().with("labelB").with("missing_label").build()
        ).withLabelsAndOp(true).build();

        assertThat(JobQueryCriteriaEvaluator.matches(job, query2)).isFalse();
    }

    @Test
    public void testMultipleLabelsWithOrOperatorCriteria() throws Exception {
        // Two labels that are present
        JobQueryCriteria<TitusTaskState, TitusJobType> query = queryBuilder.withLabels(
                expectedLabels().with("labelA").with("labelB").build()
        ).withLabelsAndOp(false).build();

        assertThat(JobQueryCriteriaEvaluator.matches(job, query)).isTrue();

        // One of the labels is missing
        JobQueryCriteria<TitusTaskState, TitusJobType> query2 = queryBuilder.withLabels(
                expectedLabels().with("labelB").with("missing_label").build()
        ).withLabelsAndOp(false).build();

        assertThat(JobQueryCriteriaEvaluator.matches(job, query2)).isTrue();
    }

    @Test
    public void testJobGroupStackCriteria() throws Exception {
        // Matching
        JobQueryCriteria<TitusTaskState, TitusJobType> query = queryBuilder.withJobGroupStack("myStack").build();
        assertThat(JobQueryCriteriaEvaluator.matches(job, query)).isTrue();

        // Not matching
        JobQueryCriteria<TitusTaskState, TitusJobType> query2 = queryBuilder.withJobGroupStack("otherStack").build();
        assertThat(JobQueryCriteriaEvaluator.matches(job, query2)).isFalse();
    }

    @Test
    public void testJobGroupDetailCriteria() throws Exception {
        // Matching
        JobQueryCriteria<TitusTaskState, TitusJobType> query = queryBuilder.withJobGroupDetail("myDetail").build();
        assertThat(JobQueryCriteriaEvaluator.matches(job, query)).isTrue();

        // Not matching
        JobQueryCriteria<TitusTaskState, TitusJobType> query2 = queryBuilder.withJobGroupDetail("otherDetail").build();
        assertThat(JobQueryCriteriaEvaluator.matches(job, query2)).isFalse();
    }

    @Test
    public void testJobGroupSequenceCriteria() throws Exception {
        // Matching
        JobQueryCriteria<TitusTaskState, TitusJobType> query = queryBuilder.withJobGroupSequence("mySequence").build();
        assertThat(JobQueryCriteriaEvaluator.matches(job, query)).isTrue();

        // Not matching
        JobQueryCriteria<TitusTaskState, TitusJobType> query2 = queryBuilder.withJobGroupSequence("otherSequence").build();
        assertThat(JobQueryCriteriaEvaluator.matches(job, query2)).isFalse();
    }

    private static LabelMapBuilder expectedLabels() {
        return new LabelMapBuilder();
    }

    private static class LabelMapBuilder {
        private final Map<String, Set<String>> labels = new HashMap<>();

        private LabelMapBuilder with(String key) {
            labels.put(key, Collections.emptySet());
            return this;
        }

        private LabelMapBuilder with(String key, String... values) {
            Set<String> valueSet = new HashSet<>();
            for (String v : values) {
                valueSet.add(v);
            }
            labels.put(key, valueSet);
            return this;
        }

        private Map<String, Set<String>> build() {
            return new HashMap<>(labels);
        }
    }
}