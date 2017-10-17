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

package io.netflix.titus.testkit.junit.jaxrs;

import java.util.Optional;

public class HttpTestClientException extends RuntimeException {

    private final int statusCode;
    private final Optional<Object> content;

    public HttpTestClientException(int statusCode, String message) {
        this(statusCode, message, null);
    }

    public HttpTestClientException(int statusCode, String message, Object content) {
        super(message);
        this.statusCode = statusCode;
        this.content = content instanceof Optional
                ? (Optional<Object>) content
                : content == null ? Optional.empty() : Optional.of(content);
    }

    public HttpTestClientException(String message, Throwable cause) {
        super(message, cause);
        this.statusCode = -1;
        this.content = Optional.empty();
    }

    public int getStatusCode() {
        return statusCode;
    }

    public Optional<Object> getContent() {
        return content;
    }
}
