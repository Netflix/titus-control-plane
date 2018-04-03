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

package com.netflix.titus.testkit.junit.resource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.rules.ExternalResource;

public class Log4jExternalResource extends ExternalResource {

    private final List<Logger> loggers = new ArrayList<>();
    private final List<Level> levels = new ArrayList<>();

    private Log4jExternalResource(List<String> categories) {
        categories.forEach(c -> {
            Logger logger = Logger.getLogger(c);
            loggers.add(logger);
            levels.add(logger.getLevel());
        });
    }

    @Override
    protected void before() throws Throwable {
        loggers.forEach(l -> l.setLevel(Level.DEBUG));
    }

    @Override
    protected void after() {
        for (int i = 0; i < loggers.size(); i++) {
            loggers.get(i).setLevel(levels.get(i));
        }
    }

    public static Log4jExternalResource enableFor(Class<?> className) {
        return new Log4jExternalResource(Collections.singletonList(className.getName()));
    }
}
