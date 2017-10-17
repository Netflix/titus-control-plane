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

package io.netflix.titus.runtime.endpoint.common;

import java.util.Optional;

/**
 * Provides location information where container log files are stored (live and persisted).
 */
public interface LogStorageInfo<TASK> {

    class LogLinks {
        private final Optional<String> liveLink;
        private final Optional<String> logLink;
        private final Optional<String> snapshotLink;

        public LogLinks(Optional<String> liveLink, Optional<String> logLink, Optional<String> snapshotLink) {
            this.liveLink = liveLink;
            this.logLink = logLink;
            this.snapshotLink = snapshotLink;
        }

        public Optional<String> getLiveLink() {
            return liveLink;
        }

        public Optional<String> getLogLink() {
            return logLink;
        }

        public Optional<String> getSnapshotLink() {
            return snapshotLink;
        }
    }

    class S3LogLocation {
        private final String accountName;
        private final String accountId;
        private final String region;
        private final String bucket;
        private final String key;

        public S3LogLocation(String accountName,
                             String accountId,
                             String region,
                             String bucket,
                             String key) {
            this.accountName = accountName;
            this.accountId = accountId;
            this.region = region;
            this.bucket = bucket;
            this.key = key;
        }

        public String getAccountName() {
            return accountName;
        }

        public String getAccountId() {
            return accountId;
        }

        public String getRegion() {
            return region;
        }

        public String getBucket() {
            return bucket;
        }

        public String getKey() {
            return key;
        }
    }

    /**
     * Get log links for a given task.
     */
    LogLinks getLinks(TASK task);

    /**
     * Link to TitusUI with the task's log page.
     */
    Optional<String> getTitusUiLink(TASK task);

    /**
     * Get S3 location containing files of a given task.
     */
    Optional<S3LogLocation> getS3LogLocation(TASK task);
}
