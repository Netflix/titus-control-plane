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

package io.netflix.titus.common.network.http;

public class Methods {

    public static final String GET = "GET";
    public static final String HEAD = "HEAD";
    public static final String POST = "POST";
    public static final String DELETE = "DELETE";
    public static final String PUT = "PUT";
    public static final String PATCH = "PATCH";
    public static final String OPTIONS = "OPTIONS";

    public static boolean isBodyAllowed(String method) {
        return method.equalsIgnoreCase(POST) || method.equalsIgnoreCase(DELETE) || method.equalsIgnoreCase(PUT)
                || method.equalsIgnoreCase(PATCH);
    }
}
