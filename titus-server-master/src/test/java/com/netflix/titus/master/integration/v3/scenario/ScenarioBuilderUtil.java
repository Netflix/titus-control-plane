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

package com.netflix.titus.master.integration.v3.scenario;

import org.junit.Test;

import static java.util.Arrays.stream;

/**
 *
 */
final class ScenarioBuilderUtil {

    static long TIMEOUT_MS = 30_000;

    static String discoverActiveTest() {
        return stream(Thread.currentThread().getStackTrace())
                .filter(seg -> {
                    try {
                        return Class.forName(seg.getClassName()).getMethod(seg.getMethodName()).getAnnotation(Test.class) != null;
                    } catch (Exception ignore) {
                        return false;
                    }
                })
                .findFirst()
                .map(StackTraceElement::getMethodName)
                .orElse("?");
    }
}
