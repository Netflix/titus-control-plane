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

package com.netflix.titus.master.eviction.service.quota.job;

import java.util.Optional;

import com.netflix.titus.master.eviction.service.quota.QuotaTracker;

/**
 * The eviction service does not impose any constraints for the self managed policy.
 */
public class SelfManagedPolicyTracker implements QuotaTracker {

    private static final SelfManagedPolicyTracker INSTANCE = new SelfManagedPolicyTracker();

    @Override
    public long getQuota() {
        return Long.MAX_VALUE / 2;
    }

    @Override
    public Optional<String> explainRestrictions(String taskId) {
        return Optional.empty();
    }

    public static SelfManagedPolicyTracker getInstance() {
        return INSTANCE;
    }
}
