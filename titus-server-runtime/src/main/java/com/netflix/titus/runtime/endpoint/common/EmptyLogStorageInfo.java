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

package com.netflix.titus.runtime.endpoint.common;

import java.util.Optional;
import javax.inject.Singleton;

@Singleton
public class EmptyLogStorageInfo<TASK> implements LogStorageInfo<TASK> {

    private static final LogLinks EMPTY = new LogLinks(Optional.empty(), Optional.empty(), Optional.empty());

    public static final LogStorageInfo INSTANCE = new EmptyLogStorageInfo();

    @Override
    public LogLinks getLinks(TASK task) {
        return EMPTY;
    }

    @Override
    public Optional<String> getTitusUiLink(TASK task) {
        return Optional.empty();
    }

    @Override
    public Optional<S3LogLocation> getS3LogLocation(TASK task) {
        return Optional.empty();
    }

    public static <TASK> LogStorageInfo<TASK> empty() {
        return INSTANCE;
    }
}
