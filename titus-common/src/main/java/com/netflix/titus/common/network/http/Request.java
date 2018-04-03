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

package com.netflix.titus.common.network.http;


import java.util.List;

public final class Request {

    private String url;
    private String method;
    private Headers headers;
    private RequestBody body;

    Request(Builder builder) {
        this.url = builder.url;
        this.method = builder.method;
        this.headers = builder.headers;
        this.body = builder.body;
    }

    public String getUrl() {
        return url;
    }

    public String getMethod() {
        return method;
    }

    public Headers getHeaders() {
        return headers;
    }

    public String getHeader(String name) {
        return headers.get(name);
    }

    public List<String> getHeaders(String name) {
        return headers.values(name);
    }

    public RequestBody getBody() {
        return body;
    }

    public Builder newBuilder() {
        return new Builder(this);
    }

    public static class Builder {
        String url;
        String method;
        Headers headers;
        RequestBody body;

        public Builder() {
            this.method = Methods.GET;
            this.headers = new Headers();
        }

        public Builder(Request request) {
            this.url = request.getUrl();
            this.method = request.getMethod();
            this.headers = request.getHeaders();
            this.body = request.getBody();
        }

        public Builder url(String url) {
            this.url = url;
            return this;
        }

        public Builder method(String method) {
            this.method = method;
            return this;
        }

        public Builder get() {
            this.method = Methods.GET;
            return this;
        }

        public Builder head() {
            this.method = Methods.HEAD;
            return this;
        }

        public Builder post() {
            this.method = Methods.POST;
            return this;
        }

        public Builder delete() {
            this.method = Methods.DELETE;
            return this;
        }

        public Builder put() {
            this.method = Methods.PUT;
            return this;
        }

        public Builder patch() {
            this.method = Methods.PATCH;
            return this;
        }

        public Builder header(String name, String value) {
            this.headers.put(name, value);
            return this;
        }

        public Builder headers(Headers headers) {
            this.headers = headers;
            return this;
        }

        public Builder body(RequestBody body) {
            this.body = body;
            return this;
        }

        public Request build() {
            return new Request(this);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Request request = (Request) o;

        if (url != null ? !url.equals(request.url) : request.url != null) {
            return false;
        }
        if (method != null ? !method.equals(request.method) : request.method != null) {
            return false;
        }
        if (headers != null ? !headers.equals(request.headers) : request.headers != null) {
            return false;
        }
        return body != null ? body.equals(request.body) : request.body == null;
    }

    @Override
    public int hashCode() {
        int result = url != null ? url.hashCode() : 0;
        result = 31 * result + (method != null ? method.hashCode() : 0);
        result = 31 * result + (headers != null ? headers.hashCode() : 0);
        result = 31 * result + (body != null ? body.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Request{" +
                "url='" + url + '\'' +
                ", method='" + method + '\'' +
                ", headers=" + headers +
                ", body=" + body +
                '}';
    }
}
