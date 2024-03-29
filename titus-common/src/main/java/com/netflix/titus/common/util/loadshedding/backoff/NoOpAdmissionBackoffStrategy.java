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

package com.netflix.titus.common.util.loadshedding.backoff;

import com.netflix.titus.common.util.loadshedding.AdaptiveAdmissionController;
import com.netflix.titus.common.util.loadshedding.AdmissionBackoffStrategy;

public class NoOpAdmissionBackoffStrategy implements AdmissionBackoffStrategy {

    private static final NoOpAdmissionBackoffStrategy INSTANCE = new NoOpAdmissionBackoffStrategy();

    @Override
    public double getThrottleFactor() {
        return 1.0;
    }

    @Override
    public void onSuccess(long elapsedMs) {
    }

    @Override
    public void onError(long elapsedMs, AdaptiveAdmissionController.ErrorKind errorKind, Throwable cause) {
    }

    public static AdmissionBackoffStrategy getInstance() {
        return INSTANCE;
    }
}
