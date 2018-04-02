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

package com.netflix.titus.common.model.sanitizer;

/**
 * Enum type for {@link EntitySanitizer} validation modes. Each {@link EntitySanitizer} instance runs either
 * in strict or permissive mode. Each {@link ClassInvariant}, and {@link FieldInvariant} validation rule is
 * configured with the validation mode as well, with the permissive mode as a default. For example,
 * if the {@link EntitySanitizer} is configured as permissive, and there is a filed with {@link ClassInvariant} annotation
 * with the {@link #Strict} mode, it will not be checked during the validation process. It would be however checked
 * if the {@link EntitySanitizer} was in the {@link #Strict} mode.
 */
public enum VerifierMode {
    /**
     * Most restrictive mode. Validation rules annotated as 'Strict' are only checked in the strict mode.
     */
    Strict {
        @Override
        public boolean includes(VerifierMode mode) {
            return true;
        }
    },

    /**
     * Default validation mode. Checks non strict validation rules only.
     */
    Permissive {
        @Override
        public boolean includes(VerifierMode mode) {
            return mode == Permissive;
        }
    };

    public abstract boolean includes(VerifierMode mode);
}
