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

package io.netflix.titus.api.json;

import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static io.netflix.titus.api.json.ObjectMappers.compactMapper;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class ObjectMappersTest {
    private final OuterClass NESTED_OBJECT = new OuterClass(
            "outerStringValue",
            123,
            new InnerClass("innerStringValue", 321),
            asList(new InnerClass("item1", 1), new InnerClass("item2", 2))
    );

    @Test
    public void testFieldsFilter() throws Exception {
        OuterClass deserialized = filter(asList("stringValue", "objectValue.intValue", "objectValues.intValue"));

        assertThat(deserialized.stringValue).isEqualTo("outerStringValue");
        assertThat(deserialized.intValue).isEqualTo(0);

        assertThat(deserialized.objectValue).isNotNull();
        assertThat(deserialized.objectValue.stringValue).isNull();
        assertThat(deserialized.objectValue.intValue).isEqualTo(321);

        assertThat(deserialized.objectValues).isNotNull();
    }

    @Test
    public void testFieldsFilterWithFullInnerObjects() throws Exception {
        OuterClass deserialized = filter(Collections.singletonList("objectValue"));
        assertThat(deserialized.objectValue).isEqualTo(NESTED_OBJECT.objectValue);
    }

    @Test
    public void testFieldsFilterWithInvalidFieldDefinition() throws Exception {
        OuterClass deserialized = filter(Collections.singletonList("objectValue.intValue.fakeField"));
        assertThat(deserialized.objectValue.intValue).isEqualTo(0);
    }

    private OuterClass filter(List<String> fields) throws Exception {
        ObjectMapper mapper = ObjectMappers.applyFieldsFilter(compactMapper(), fields);

        String jsonValue = mapper.writeValueAsString(NESTED_OBJECT);
        return compactMapper().readValue(jsonValue, OuterClass.class);
    }

    private static class OuterClass {
        @JsonProperty
        private final String stringValue;

        @JsonProperty
        private final int intValue;

        @JsonProperty
        private final InnerClass objectValue;

        @JsonProperty
        private final List<InnerClass> objectValues;

        @JsonCreator
        private OuterClass(@JsonProperty("stringValue") String stringValue,
                           @JsonProperty("intValue") int intValue,
                           @JsonProperty("objectValue") InnerClass objectValue,
                           @JsonProperty("objectValues") List<InnerClass> objectValues) {
            this.stringValue = stringValue;
            this.intValue = intValue;
            this.objectValue = objectValue;
            this.objectValues = objectValues;
        }
    }

    private static class InnerClass {
        @JsonProperty
        private final String stringValue;

        @JsonProperty
        private final int intValue;

        @JsonCreator
        private InnerClass(@JsonProperty("stringValue") String stringValue,
                           @JsonProperty("intValue") int intValue) {
            this.stringValue = stringValue;
            this.intValue = intValue;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            InnerClass that = (InnerClass) o;

            if (intValue != that.intValue) {
                return false;
            }
            return stringValue != null ? stringValue.equals(that.stringValue) : that.stringValue == null;
        }

        @Override
        public String toString() {
            return "InnerClass{stringValue='" + stringValue + '\'' + ", intValue=" + intValue + '}';
        }
    }
}