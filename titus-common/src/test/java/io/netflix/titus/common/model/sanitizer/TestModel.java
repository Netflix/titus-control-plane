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

package io.netflix.titus.common.model.sanitizer;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

import com.google.common.base.Preconditions;

public class TestModel {

    @NeverNull
    @ClassInvariant(condition = "#fit()", message = "Simulated failure (OS=#{@env['os.name']})")
    public static class Root {

        @FieldInvariant(value = "value == null || T(java.lang.Character).isUpperCase(value.charAt(0))", message = "Name '#{value}' must start with a capital letter")
        private final String name;

        @FieldSanitizer(sanitizer = MinMaxOrderSanitizer.class)
        @Valid
        private final Child child;

        @Valid
        private final NullableChild nullableChild;

        public Root(String name, Child child, NullableChild nullableChild) {
            this.name = name;
            this.child = child;
            this.nullableChild = nullableChild;
        }

        public String getName() {
            return name;
        }

        public Child getChild() {
            return child;
        }

        public NullableChild getNullableChild() {
            return nullableChild;
        }
    }

    @NeverNull
    @ClassInvariant(condition = "min <= max", message = "'min'(#{min} must be <= 'max'(#{max}")
    public static class Child {

        @Template
        private final String childName;

        @Min(value = 0, message = "'min' must be >= 0, but is #{#root}")
        @FieldInvariant(value = "value % 2 == 0", message = "'min' must be even number")
        private final int min;

        @FieldSanitizer(atLeast = 10, atMost = 100, adjuster = "(value / 2) * 2")
        @Max(100)
        private final int max;

        public Child(String childName, int min, int max) {
            this.childName = childName;
            this.min = min;
            this.max = max;
        }

        public String getChildName() {
            return childName;
        }

        public int getMin() {
            return min;
        }

        public int getMax() {
            return max;
        }
    }

    public static class NullableChild {
        private final String optionalValue;

        public NullableChild(String optionalValue) {
            this.optionalValue = optionalValue;
        }

        public String getOptionalValue() {
            return optionalValue;
        }
    }

    public static class ChildExt extends Child {

        private final int desired;

        public ChildExt(String childName, int min, int desired, int max) {
            super(childName, min, max);
            this.desired = desired;
        }

        public int getDesired() {
            return desired;
        }
    }

    public static class CollectionHolder {

        @Template
        private final List<String> values;

        public CollectionHolder(List<String> values) {
            this.values = values;
        }

        public List<String> getValues() {
            return values;
        }
    }

    public static class StringWithPrefixCheck {

        @FieldInvariant("@myObj.containsPrefix(value)")
        private final String value;

        public StringWithPrefixCheck(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    public static final class MinMaxOrderSanitizer implements Function<Object, Optional<Object>> {

        @Override
        public Optional<Object> apply(Object childObject) {
            Preconditions.checkArgument(childObject instanceof Child);

            Child child = (Child) childObject;
            if (child.getMax() < child.getMin()) {
                return Optional.of(new Child(child.getChildName(), child.getMin(), child.getMin()));
            }
            return Optional.empty();
        }
    }

    private static ThreadLocal<Boolean> fitStatus = new ThreadLocal<>();

    public static void setFit(boolean status) {
        fitStatus.set(status);
    }

    public static boolean fit() {
        return fitStatus.get() == null || fitStatus.get();
    }

    public static Method getFitMethod() {
        try {
            return TestModel.class.getDeclaredMethod("fit");
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
    }

    public static class SampleValidationMethods {

        private final String prefix;

        public SampleValidationMethods(String prefix) {
            this.prefix = prefix;
        }

        public Boolean containsPrefix(String stringValue) {
            return stringValue != null && stringValue.contains(stringValue);
        }
    }
}
