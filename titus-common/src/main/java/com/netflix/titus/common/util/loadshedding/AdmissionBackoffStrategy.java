/*
 * Copyright 2022 Netflix, Inc.
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

package com.netflix.titus.common.util.loadshedding;

/**
 * Based on observed success / error invocations, computes an adjustment factor for an admission control rate.
 */
public interface AdmissionBackoffStrategy {

    /**
     * In range 0-1. For example, if admission rate is 100, and throttle factor is 0.5, the effective admission
     * rate is computed as 100 * 0.5 = 50.
     */
    double getThrottleFactor();

    void onSuccess(long elapsedMs);

    void onError(long elapsedMs, AdaptiveAdmissionController.ErrorKind errorKind, Throwable cause);
}
