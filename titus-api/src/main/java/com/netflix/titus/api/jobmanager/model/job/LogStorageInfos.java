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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.LogStorageInfo.S3LogLocation;
import com.netflix.titus.common.util.StringExt;

/**
 * Helper functions to process log store related data.
 */
public final class LogStorageInfos {

    private static final Pattern S3_ACCESS_POINT_ARN_RE = Pattern.compile(
            "arn:aws:s3:([^:]+):([^:]+):accesspoint/.*"
    );

    /**
     * We copy the custom S3 log location into the task, so we can build {@link LogStorageInfo} without having the {@link Job} object.
     * This is an optimization that avoids reading job records from the archive store, when tasks are read.
     */
    public static <E extends JobDescriptor.JobDescriptorExt> Map<String, String> toS3LogLocationTaskContext(Job<E> job) {
        return findCustomS3Bucket(job).map(logLocation -> {
            Map<String, String> context = new HashMap<>();
            context.put(TaskAttributes.TASK_ATTRIBUTE_S3_BUCKET_NAME, logLocation.getBucketName());
            context.put(TaskAttributes.TASK_ATTRIBUTE_S3_PATH_PREFIX, logLocation.getPathPrefix());
            return context;
        }).orElse(Collections.emptyMap());
    }

    /**
     * Build a {@link S3LogLocation} for a task. If custom S3 log location is not configured, the default value is used.
     */
    public static S3LogLocation buildLogLocation(Task task, S3LogLocation defaultLogLocation) {
        S3Bucket customS3Bucket = findCustomS3Bucket(task).orElse(null);
        if (customS3Bucket == null) {
            return defaultLogLocation;
        }

        return customS3Bucket.getS3Account()
                .map(account ->
                        new S3LogLocation(
                                account.getAccountId(),
                                account.getAccountId(),
                                account.getRegion(),
                                customS3Bucket.getBucketName(),
                                customS3Bucket.getPathPrefix()
                        ))
                .orElseGet(() ->
                        new S3LogLocation(
                                defaultLogLocation.getAccountName(),
                                defaultLogLocation.getAccountId(),
                                defaultLogLocation.getRegion(),
                                customS3Bucket.getBucketName(),
                                customS3Bucket.getPathPrefix()
                        ));
    }

    public static Optional<S3Bucket> findCustomS3Bucket(Job<?> job) {
        Map<String, String> attributes = job.getJobDescriptor().getContainer().getAttributes();
        String bucketName = attributes.get(JobAttributes.JOB_CONTAINER_ATTRIBUTE_S3_BUCKET_NAME);
        String pathPrefix = attributes.get(JobAttributes.JOB_CONTAINER_ATTRIBUTE_S3_PATH_PREFIX);
        if (StringExt.isEmpty(bucketName) || StringExt.isEmpty(pathPrefix)) {
            return Optional.empty();
        }
        return Optional.of(new S3Bucket(bucketName, pathPrefix));
    }

    public static Optional<S3Bucket> findCustomS3Bucket(Task task) {
        Map<String, String> attributes = task.getTaskContext();
        String bucketName = attributes.get(TaskAttributes.TASK_ATTRIBUTE_S3_BUCKET_NAME);
        String pathPrefix = attributes.get(TaskAttributes.TASK_ATTRIBUTE_S3_PATH_PREFIX);
        if (StringExt.isEmpty(bucketName) || StringExt.isEmpty(pathPrefix)) {
            return Optional.empty();
        }
        return Optional.of(new S3Bucket(bucketName, pathPrefix));
    }

    /**
     * S3 access point ARN format: arn:aws:s3:region:account-id:accesspoint/resource
     * For example: arn:aws:s3:us-west-2:123456789012:accesspoint/test
     * If the bucket name is ARN, we use region/account id from the ARN.
     */
    @VisibleForTesting
    static Optional<S3Account> parseS3AccessPointArn(String s3AccessPointArn) {
        String trimmed = StringExt.safeTrim(s3AccessPointArn);
        if (trimmed.isEmpty()) {
            return Optional.empty();
        }
        Matcher matcher = S3_ACCESS_POINT_ARN_RE.matcher(s3AccessPointArn);
        if (!matcher.matches()) {
            return Optional.empty();
        }
        return Optional.of(new S3Account(matcher.group(2), matcher.group(1)));
    }

    public static class S3Account {

        private final String accountId;
        private final String region;

        public S3Account(String accountId, String region) {
            this.accountId = accountId;
            this.region = region;
        }

        public String getAccountId() {
            return accountId;
        }

        public String getRegion() {
            return region;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            S3Account s3Account = (S3Account) o;
            return Objects.equals(accountId, s3Account.accountId) &&
                    Objects.equals(region, s3Account.region);
        }

        @Override
        public int hashCode() {
            return Objects.hash(accountId, region);
        }
    }

    public static class S3Bucket {

        private final String bucketName;
        private final String pathPrefix;
        private final Optional<S3Account> s3Account;

        public S3Bucket(String bucketName, String pathPrefix) {
            this.bucketName = bucketName;
            this.pathPrefix = pathPrefix;
            this.s3Account = parseS3AccessPointArn(bucketName);
        }

        public Optional<S3Account> getS3Account() {
            return s3Account;
        }

        public String getBucketName() {
            return bucketName;
        }

        public String getPathPrefix() {
            return pathPrefix;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            S3Bucket s3Bucket = (S3Bucket) o;
            return Objects.equals(bucketName, s3Bucket.bucketName) &&
                    Objects.equals(pathPrefix, s3Bucket.pathPrefix) &&
                    Objects.equals(s3Account, s3Bucket.s3Account);
        }

        @Override
        public int hashCode() {
            return Objects.hash(bucketName, pathPrefix, s3Account);
        }

        @Override
        public String toString() {
            return "S3Bucket{" +
                    "bucketName='" + bucketName + '\'' +
                    ", pathPrefix='" + pathPrefix + '\'' +
                    ", s3Account=" + s3Account +
                    '}';
        }
    }
}
