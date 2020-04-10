/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.api.jobmanager.model.job;

import java.util.HashMap;
import java.util.Map;

import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.LogStorageInfo.S3LogLocation;
import com.netflix.titus.api.jobmanager.model.job.LogStorageInfos.S3Account;
import com.netflix.titus.api.jobmanager.model.job.LogStorageInfos.S3Bucket;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class LogStorageInfosTest {

    private static final S3LogLocation DEFAULT_LOG_LOCATION = new S3LogLocation(
            "defaultAccountName",
            "defaultAccountId",
            "defaultRegion",
            "defaultBucket",
            "defaultKey"
    );

    @Test
    public void testToS3LogLocationTaskContext() {
        // Empty context
        assertS3LogLocationInTaskContextNotSet(LogStorageInfos.toS3LogLocationTaskContext(JobGenerator.oneBatchJob()));

        // Non empty context
        assertS3LogLocationInTaskContext(
                LogStorageInfos.toS3LogLocationTaskContext(newJob("bucketA", "pathB")),
                "bucketA",
                "pathB"
        );
    }

    @Test
    public void testFindCustomS3BucketInJob() {
        // Empty context
        assertThat(LogStorageInfos.findCustomS3Bucket(JobGenerator.oneBatchJob())).isEmpty();

        // Non empty context
        S3Bucket s3Bucket = LogStorageInfos.findCustomS3Bucket(newJob("bucketA", "pathB")).orElse(null);
        assertThat(s3Bucket.getBucketName()).isEqualTo("bucketA");
        assertThat(s3Bucket.getPathPrefix()).isEqualTo("pathB");
    }

    @Test
    public void testFindCustomS3BucketInTask() {
        // Empty context
        assertThat(LogStorageInfos.findCustomS3Bucket(JobGenerator.oneBatchTask())).isEmpty();

        // Non empty context
        S3Bucket s3Bucket = LogStorageInfos.findCustomS3Bucket(newTask("bucketA", "pathB")).orElse(null);
        assertThat(s3Bucket.getBucketName()).isEqualTo("bucketA");
        assertThat(s3Bucket.getPathPrefix()).isEqualTo("pathB");
    }

    @Test
    public void testParseS3AccessPointArn() {
        // No ARN
        assertThat(LogStorageInfos.parseS3AccessPointArn("myBucket")).isEmpty();

        // Good ARN
        assertThat(LogStorageInfos.parseS3AccessPointArn("arn:aws:s3:us-west-2:123456789012:accesspoint/test")).contains(
                new S3Account("123456789012", "us-west-2")
        );

        // Bad ARN
        assertThat(LogStorageInfos.parseS3AccessPointArn("arn:aws:ec2:us-west-2:123456789012:accesspoint/test")).isEmpty();
    }

    @Test
    public void testBuildLogLocation() {
        // No bucket override
        assertThat(LogStorageInfos.buildLogLocation(JobGenerator.oneBatchTask(), DEFAULT_LOG_LOCATION)).isEqualTo(DEFAULT_LOG_LOCATION);

        // Plain custom bucket name
        assertThat(LogStorageInfos.buildLogLocation(newTask("bucketA", "pathB"), DEFAULT_LOG_LOCATION)).isEqualTo(
                new S3LogLocation(
                        DEFAULT_LOG_LOCATION.getAccountName(),
                        DEFAULT_LOG_LOCATION.getAccountId(),
                        DEFAULT_LOG_LOCATION.getRegion(),
                        "bucketA",
                        "pathB"
                )
        );

        // Custom bucket name with S3 ARN.
        assertThat(LogStorageInfos.buildLogLocation(
                newTask("arn:aws:s3:us-west-2:123456789012:accesspoint/test", "pathB"),
                DEFAULT_LOG_LOCATION)
        ).isEqualTo(
                new S3LogLocation(
                        "123456789012",
                        "123456789012",
                        "us-west-2",
                        "arn:aws:s3:us-west-2:123456789012:accesspoint/test",
                        "pathB"
                )
        );
    }

    private Job newJob(String bucketName, String pathPrefix) {
        Job<BatchJobExt> job = JobGenerator.oneBatchJob();

        Map<String, String> containerAttributes = new HashMap<>(job.getJobDescriptor().getContainer().getAttributes());
        containerAttributes.put(JobAttributes.JOB_CONTAINER_ATTRIBUTE_S3_BUCKET_NAME, bucketName);
        containerAttributes.put(JobAttributes.JOB_CONTAINER_ATTRIBUTE_S3_PATH_PREFIX, pathPrefix);

        return job.toBuilder()
                .withJobDescriptor(job.getJobDescriptor().toBuilder()
                        .withContainer(job.getJobDescriptor().getContainer().toBuilder()
                                .withAttributes(containerAttributes)
                                .build()
                        )
                        .build()
                )
                .build();
    }

    private Task newTask(String bucketName, String pathPrefix) {
        Task task = JobGenerator.oneBatchTask();

        Map<String, String> taskContext = new HashMap<>(task.getTaskContext());
        taskContext.put(TaskAttributes.TASK_ATTRIBUTE_S3_BUCKET_NAME, bucketName);
        taskContext.put(TaskAttributes.TASK_ATTRIBUTE_S3_PATH_PREFIX, pathPrefix);

        return task.toBuilder().withTaskContext(taskContext).build();
    }

    private void assertS3LogLocationInTaskContextNotSet(Map<String, String> taskContext) {
        assertThat(taskContext).doesNotContainKey(TaskAttributes.TASK_ATTRIBUTE_S3_BUCKET_NAME);
        assertThat(taskContext).doesNotContainKey(TaskAttributes.TASK_ATTRIBUTE_S3_PATH_PREFIX);
    }

    private void assertS3LogLocationInTaskContext(Map<String, String> taskContext, String bucketName, String pathPrefix) {
        assertThat(taskContext).containsEntry(TaskAttributes.TASK_ATTRIBUTE_S3_BUCKET_NAME, bucketName);
        assertThat(taskContext).containsEntry(TaskAttributes.TASK_ATTRIBUTE_S3_PATH_PREFIX, pathPrefix);
    }
}