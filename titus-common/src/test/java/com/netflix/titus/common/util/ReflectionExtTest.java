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

package com.netflix.titus.common.util;

import java.lang.reflect.Method;
import java.util.List;
import javax.annotation.PostConstruct;

import org.junit.Test;

import static com.netflix.titus.common.util.ReflectionExt.isNumeric;
import static org.assertj.core.api.Assertions.assertThat;

public class ReflectionExtTest {

    @Test
    public void testFindAnnotatedMethods() throws Exception {
        List<Method> annotatedMethods = ReflectionExt.findAnnotatedMethods(new MyServiceImpl(), PostConstruct.class);
        assertThat(annotatedMethods).hasSize(1);
        assertThat(annotatedMethods.get(0).getName()).isEqualTo("classAnnotated");
    }

    @Test
    public void testIsNumeric() throws Exception {
        assertThat(isNumeric(Integer.class)).isTrue();
        assertThat(isNumeric(Long.class)).isTrue();
        assertThat(isNumeric(IntegerHolder.class.getDeclaredField("intValue").getType())).isTrue();
    }

    static class IntegerHolder {
        int intValue;
    }

    interface MyService {
        @PostConstruct
        void interfaceAnnotated();

        void classAnnotated();

        void notAnnotated();
    }

    static class MyServiceImpl implements MyService {
        public void interfaceAnnotated() {
        }

        @PostConstruct
        public void classAnnotated() {
        }

        public void notAnnotated() {
        }
    }
}