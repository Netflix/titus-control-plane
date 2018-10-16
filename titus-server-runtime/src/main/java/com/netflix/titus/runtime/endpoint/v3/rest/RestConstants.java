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

package com.netflix.titus.runtime.endpoint.v3.rest;

import java.util.Set;

import static com.netflix.titus.common.util.CollectionsExt.asSet;

public class RestConstants {
    static final Set<String> IGNORED_QUERY_PARAMS = asSet(
            "debug", "fields", "page", "pagesize", "cursor", "accesstoken"
    );

    public static final String PAGE_QUERY_KEY = "page";
    public static final String PAGE_SIZE_QUERY_KEY = "pageSize";
    public static final String CURSOR_QUERY_KEY = "cursor";
    public static final String FIELDS_QUERY_KEY = "fields";
}
