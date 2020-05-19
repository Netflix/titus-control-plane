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

package com.netflix.titus.common.model.admission;

import java.util.Set;

import com.netflix.titus.common.model.sanitizer.ValidationError;
import reactor.core.publisher.Mono;

/**
 * An <tt>AdmissionValidator</tt> determines whether an object of the parameterized type is valid.  If it finds an
 * object to be invalid it returns a non-empty set of {@link ValidationError}s.
 *
 * @param <T> The type of object this AdmissionValidator validates.
 */
public interface AdmissionValidator<T> {
    Mono<Set<ValidationError>> validate(T entity);

    /**
     * Returns the error type this validator will produce in the {@link AdmissionValidator#validate(Object)} method.
     */
    ValidationError.Type getErrorType();
}
