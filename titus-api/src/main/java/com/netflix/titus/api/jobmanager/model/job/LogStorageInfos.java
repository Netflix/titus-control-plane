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
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.LogStorageInfo.S3LogLocation;
import com.netflix.titus.common.util.Evaluators;
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
        Map<String, String> context = new HashMap<>();

        Map<String, String> attributes = job.getJobDescriptor().getContainer().getAttributes();
        Evaluators.applyNotNull(
                attributes.get(JobAttributes.JOB_CONTAINER_ATTRIBUTE_S3_BUCKET_NAME),
                value -> context.put(TaskAttributes.TASK_ATTRIBUTE_S3_BUCKET_NAME, value)
        );
        Evaluators.applyNotNull(
                attributes.get(JobAttributes.JOB_CONTAINER_ATTRIBUTE_S3_PATH_PREFIX),
                value -> context.put(TaskAttributes.TASK_ATTRIBUTE_S3_PATH_PREFIX, value)
        );

        return context;
    }

    /**
     * Build a {@link S3LogLocation} for a task. If custom S3 log location is not configured, the default value is used.
     */
    public static S3LogLocation buildLogLocation(Task task, S3LogLocation defaultLogLocation) {
        S3Bucket customS3Bucket = findCustomS3Bucket(task).orElse(null);
        String customPathPrefix = findCustomPathPrefix(task).orElse(null);

        if (customS3Bucket == null && customPathPrefix == null) {
            return defaultLogLocation;
        }

        String fullPrefix = customPathPrefix == null
                ? defaultLogLocation.getKey()
                : buildPathPrefix(customPathPrefix, defaultLogLocation.getKey());


        if (customS3Bucket == null) {
            return new S3LogLocation(
                    defaultLogLocation.getAccountName(),
                    defaultLogLocation.getAccountId(),
                    defaultLogLocation.getRegion(),
                    defaultLogLocation.getBucket(),
                    fullPrefix
            );
        }

        return customS3Bucket.getS3Account()
                .map(account ->
                        new S3LogLocation(
                                account.getAccountId(),
                                account.getAccountId(),
                                account.getRegion(),
                                customS3Bucket.getBucketName(),
                                fullPrefix
                        ))
                .orElseGet(() ->
                        new S3LogLocation(
                                defaultLogLocation.getAccountName(),
                                defaultLogLocation.getAccountId(),
                                defaultLogLocation.getRegion(),
                                customS3Bucket.getBucketName(),
                                fullPrefix
                        ));
    }

    public static Optional<S3Bucket> findCustomS3Bucket(Job<?> job) {
        return findCustomS3Bucket(job.getJobDescriptor());
    }

    public static Optional<S3Bucket> findCustomS3Bucket(JobDescriptor<?> jobDescriptor) {
        Map<String, String> attributes = jobDescriptor.getContainer().getAttributes();
        String bucketName = attributes.get(JobAttributes.JOB_CONTAINER_ATTRIBUTE_S3_BUCKET_NAME);
        String pathPrefix = attributes.get(JobAttributes.JOB_CONTAINER_ATTRIBUTE_S3_PATH_PREFIX);
        if (StringExt.isEmpty(bucketName) || StringExt.isEmpty(pathPrefix)) {
            return Optional.empty();
        }
        return Optional.of(new S3Bucket(bucketName));
    }

    public static Optional<S3Bucket> findCustomS3Bucket(Task task) {
        Map<String, String> attributes = task.getTaskContext();
        String bucketName = attributes.get(TaskAttributes.TASK_ATTRIBUTE_S3_BUCKET_NAME);

        return StringExt.isEmpty(bucketName) ? Optional.empty() : Optional.of(new S3Bucket(bucketName));
    }

    public static Optional<String> findCustomPathPrefix(JobDescriptor<?> jobDescriptor) {
        Map<String, String> attributes = jobDescriptor.getContainer().getAttributes();
        String pathPrefix = attributes.get(JobAttributes.JOB_CONTAINER_ATTRIBUTE_S3_PATH_PREFIX);
        return StringExt.isEmpty(pathPrefix) ? Optional.empty() : Optional.of(pathPrefix);
    }

    public static Optional<String> findCustomPathPrefix(Task task) {
        return Optional.ofNullable(task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTE_S3_PATH_PREFIX));
    }

    public static String buildPathPrefix(String first, String second) {
        if (StringExt.isEmpty(second)) {
            return first;
        }
        if (first.startsWith("/")) {
            return first + second;
        }
        return first + '/' + second;
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
        private final Optional<S3Account> s3Account;

        public S3Bucket(String bucketName) {
            this.bucketName = bucketName;
            this.s3Account = parseS3AccessPointArn(bucketName);
        }

        public Optional<S3Account> getS3Account() {
            return s3Account;
        }

        public String getBucketName() {
            return bucketName;
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
                    Objects.equals(s3Account, s3Bucket.s3Account);
        }

        @Override
        public int hashCode() {
            return Objects.hash(bucketName, s3Account);
        }

        @Override
        public String toString() {
            return "S3Bucket{" +
                    "bucketName='" + bucketName + '\'' +
                    ", s3Account=" + s3Account +
                    '}';
        }
    }
}
