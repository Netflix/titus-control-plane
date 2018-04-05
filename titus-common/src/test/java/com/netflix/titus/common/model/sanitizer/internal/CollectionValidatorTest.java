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

package com.netflix.titus.common.model.sanitizer.internal;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.validation.ConstraintViolation;
import javax.validation.Validator;

import com.google.common.collect.ImmutableMap;
import com.netflix.titus.common.model.sanitizer.CollectionInvariants;
import com.netflix.titus.common.model.sanitizer.TestValidator;
import org.junit.Before;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class CollectionValidatorTest {

    private Validator validator;

    @Before
    public void setUp() throws Exception {
        validator = TestValidator.testStrictValidator();
    }

    @Test
    public void testCollection() throws Exception {
        assertThat(validator.validate(new ListWrapper(asList("A", "A", "C")))).isEmpty();

        Set<ConstraintViolation<ListWrapper>> violations = validator.validate(new ListWrapper(asList("A", null, "C")));
        System.out.println(violations);
        assertThat(violations).hasSize(1);
    }

    @Test
    public void testMap() throws Exception {
        assertThat(validator.validate(new MapWrapper(ImmutableMap.of(
                "k1", "v1",
                "k2", "v2",
                "k3", "v3"
        )))).isEmpty();

        Map<String, String> mapWithNullValues = new HashMap<>();
        mapWithNullValues.put("k1", "v1");
        mapWithNullValues.put("k2", null);
        mapWithNullValues.put(null, "v3");
        Set<ConstraintViolation<MapWrapper>> violations = validator.validate(new MapWrapper(mapWithNullValues));
        System.out.println(violations);
        assertThat(violations).hasSize(1);
    }

    static class ListWrapper {

        @CollectionInvariants(message = "Collection contains null values")
        List<String> values;

        ListWrapper(List<String> values) {
            this.values = values;
        }
    }

    static class MapWrapper {
        @CollectionInvariants(message = "Map contains null keys or values")
        Map<String, String> values;

        MapWrapper(Map<String, String> values) {
            this.values = values;
        }

    }
}