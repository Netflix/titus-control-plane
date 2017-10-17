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

package io.netflix.titus.common.network.http.internal.okhttp;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netflix.titus.common.network.http.Headers;
import io.netflix.titus.common.network.http.Request;
import io.netflix.titus.common.network.http.RequestBody;
import io.netflix.titus.common.network.http.Response;
import io.netflix.titus.common.network.http.ResponseBody;
import io.netflix.titus.common.network.http.StatusCode;
import okhttp3.MediaType;
import okhttp3.internal.Util;
import okio.BufferedSink;
import okio.BufferedSource;
import okio.Okio;
import okio.Source;

import static okhttp3.Protocol.HTTP_1_1;

class OkHttpConverters {

    private static ObjectMapper objectMapper = new ObjectMapper();

    static okhttp3.Request toOkHttpRequest(Request request) {
        okhttp3.RequestBody requestBody = request.getBody() == null ? null : toOkHttpRequestBody(request.getBody());
        okhttp3.Request.Builder builder = new okhttp3.Request.Builder()
                .url(request.getUrl())
                .method(request.getMethod(), requestBody)
                .headers(toOkHttpHeaders(request.getHeaders()));

        return builder.build();
    }

    static Request fromOkHttpRequest(okhttp3.Request request) {
        Request.Builder builder = new Request.Builder()
                .url(request.url().toString())
                .method(request.method())
                .headers(fromOkHttpHeaders(request.headers()));

        if (request.body() != null) {
            builder.body(RequestBody.create(request.body()));
        }

        return builder.build();
    }

    static okhttp3.Response toOkHttpResponse(Response response) {
        okhttp3.Response.Builder builder = new okhttp3.Response.Builder()
                .code(response.getStatusCode().getCode())
                .headers(toOkHttpHeaders(response.getHeaders()))
                .protocol(HTTP_1_1);

        if (response.getRequest() != null) {
            builder.request(toOkHttpRequest(response.getRequest()));
        }
        if (response.hasBody()) {
            builder.body(toOkHttpResponseBody(response.getBody()));
        }

        return builder.build();
    }

    static Response fromOkHttpResponse(okhttp3.Response response) {
        Response.Builder builder = new Response.Builder()
                .headers(fromOkHttpHeaders(response.headers()))
                .statusCode(StatusCode.fromCode(response.code()))
                .body(fromOkHttpResponseBody(response.body()))
                .sentRequestAtMillis(response.sentRequestAtMillis())
                .receivedResponseAtMillis(response.receivedResponseAtMillis());

        if (response.request() != null) {
            builder.request(fromOkHttpRequest(response.request()));
        }
        return builder.build();
    }

    static okhttp3.Headers toOkHttpHeaders(Headers headers) {
        okhttp3.Headers.Builder builder = new okhttp3.Headers.Builder();
        headers.names().forEach(name -> {
            List<String> values = headers.values(name);
            if (values != null) {
                values.forEach(value -> builder.add(name, value));
            }
        });
        return builder.build();
    }

    static Headers fromOkHttpHeaders(okhttp3.Headers headers) {
        Headers newHeaders = new Headers();
        headers.names().forEach(name -> {
            List<String> values = headers.values(name);
            if (values != null && !values.isEmpty()) {
                newHeaders.set(name, values);
            }
        });
        return newHeaders;
    }

    static okhttp3.RequestBody toOkHttpRequestBody(RequestBody body) {
        Object object = body.get();
        if (object instanceof InputStream) {
            InputStream inputStream = (InputStream) object;

            return new okhttp3.RequestBody() {
                @Override
                public MediaType contentType() {
                    return null;
                }

                @Override
                public long contentLength() {
                    return -1L;
                }

                @Override
                public void writeTo(BufferedSink sink) throws IOException {
                    Source source = null;
                    try {
                        source = Okio.source(inputStream);
                        sink.writeAll(source);
                    } finally {
                        Util.closeQuietly(source);
                    }
                }
            };

        } else if (object instanceof String) {
            String string = (String) object;
            return okhttp3.RequestBody.create(null, string);
        } else {
            return null;
        }
    }

    static okhttp3.ResponseBody toOkHttpResponseBody(ResponseBody body) {
        BufferedSource bufferedSource = Okio.buffer(Okio.source(body.get(InputStream.class)));
        return okhttp3.ResponseBody.create(null, -1L, bufferedSource);
    }

    static ResponseBody fromOkHttpResponseBody(okhttp3.ResponseBody body) {
        return new ResponseBody() {
            @Override
            public <T> T get(Class<T> type) {
                if (type.isAssignableFrom(InputStream.class)) {
                    return type.cast(body.byteStream());
                } else if (type.isAssignableFrom(String.class)) {
                    try {
                        return type.cast(body.string());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    try {
                        return objectMapper.readValue(body.byteStream(), type);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    } finally {
                        body.close();
                    }
                }
            }

            @Override
            public long getContentLength() {
                return body.contentLength();
            }

            @Override
            public void close() {
                body.close();
            }
        };
    }
}
