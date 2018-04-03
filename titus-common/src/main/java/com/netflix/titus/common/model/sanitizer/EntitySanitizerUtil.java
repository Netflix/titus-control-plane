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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.validation.ConstraintViolation;

/**
 * Collection of helper functions.
 */
public final class EntitySanitizerUtil {

    private EntitySanitizerUtil() {
    }

    public static Map<String, String> toStringMap(Collection<ConstraintViolation<?>> violations) {
        if (violations == null) {
            return Collections.emptyMap();
        }
        Map<String, String> violationsMap = new HashMap<>();
        for (ConstraintViolation<?> violation : violations) {
            Object message = violation.getMessage();
            if (message != null) {
                violationsMap.put(violation.getPropertyPath().toString(), message.toString());
            }
        }
        return violationsMap;
    }
}
