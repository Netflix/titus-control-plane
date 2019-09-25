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

import static com.netflix.titus.common.util.CollectionsExt.first;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class CollectionValidatorTest {

    private Validator validator;

    @Before
    public void setUp() {
        validator = TestValidator.testStrictValidator();
    }

    @Test
    public void testValidCollection() {
        assertThat(validator.validate(new ListWrapper(asList("A", "A", "C")))).isEmpty();
    }

    @Test
    public void testCollectionWithNullValues() {
        Set<ConstraintViolation<ListWrapper>> violations = validator.validate(new ListWrapper(asList("A", null, "C")));
        assertThat(violations).hasSize(1);
        assertThat(first(violations).getMessage()).isEqualTo("null values not allowed");
    }

    @Test
    public void testValidMap() {
        assertThat(validator.validate(new MapWrapper(ImmutableMap.of(
                "k1", "v1",
                "k2", "v2",
                "k3", "v3"
        )))).isEmpty();
    }

    @Test
    public void testMapWithEmptyKeys() {
        Map<String, String> mapWithEmptyKeys = new HashMap<>();
        mapWithEmptyKeys.put("k1", "v1");
        mapWithEmptyKeys.put("", "v2");

        Set<ConstraintViolation<MapWrapper>> violations = validator.validate(new MapWrapper(mapWithEmptyKeys));
        assertThat(violations).hasSize(1);
        assertThat(first(violations).getMessage()).isEqualTo("empty key names not allowed");
    }

    @Test
    public void testMapWithNullKeys() {
        Map<String, String> mapWithNullKeys = new HashMap<>();
        mapWithNullKeys.put("k1", "v1");
        mapWithNullKeys.put(null, "v2");

        Set<ConstraintViolation<MapWrapperWithEmptyKeys>> violations = validator.validate(new MapWrapperWithEmptyKeys(mapWithNullKeys));
        assertThat(violations).hasSize(1);
        assertThat(first(violations).getMessage()).isEqualTo("null key names not allowed");
    }

    @Test
    public void testMapWithNullValues() {
        Map<String, String> mapWithNullValues = new HashMap<>();
        mapWithNullValues.put("k1", "v1");
        mapWithNullValues.put("k2", null);
        mapWithNullValues.put("k3", null);

        Set<ConstraintViolation<MapWrapper>> violations = validator.validate(new MapWrapper(mapWithNullValues));
        assertThat(violations).hasSize(1);
        assertThat(first(violations).getMessage()).isEqualTo("null values found for keys: [k2, k3]");
    }

    static class ListWrapper {

        @CollectionInvariants
        List<String> values;

        ListWrapper(List<String> values) {
            this.values = values;
        }
    }

    static class MapWrapper {
        @CollectionInvariants(allowEmptyKeys = false)
        Map<String, String> values;

        MapWrapper(Map<String, String> values) {
            this.values = values;
        }

    }

    static class MapWrapperWithEmptyKeys {
        @CollectionInvariants(allowEmptyKeys = true, allowNullValues = false)
        Map<String, String> values;

        MapWrapperWithEmptyKeys(Map<String, String> values) {
            this.values = values;
        }

    }
}