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

public class Response {
    private Request request;
    private Headers headers;
    private StatusCode statusCode;
    private ResponseBody body;
    private long sentRequestAtMillis;
    private long receivedResponseAtMillis;

    Response(Builder builder) {
        this.request = builder.request;
        this.headers = builder.headers;
        this.statusCode = builder.statusCode;
        this.body = builder.body;
        this.sentRequestAtMillis = builder.sentRequestAtMillis;
        this.receivedResponseAtMillis = builder.receivedResponseAtMillis;
    }

    public Request getRequest() {
        return request;
    }

    public String getHeader(String name) {
        return headers.get(name);
    }

    public Headers getHeaders() {
        return headers;
    }

    public StatusCode getStatusCode() {
        return statusCode;
    }

    public boolean isSuccessful() {
        return statusCode.getStatusClass() == StatusCode.StatusClass.SUCCESSFUL;
    }

    public boolean hasBody() {
        return body != null;
    }

    public ResponseBody getBody() {
        return body;
    }

    public long getSentRequestAtMillis() {
        return sentRequestAtMillis;
    }

    public long getReceivedResponseAtMillis() {
        return receivedResponseAtMillis;
    }

    public Builder newBuilder() {
        return new Builder(this);
    }

    public static class Builder {

        Request request;
        Headers headers;
        StatusCode statusCode;
        ResponseBody body;
        long sentRequestAtMillis;
        long receivedResponseAtMillis;

        public Builder() {
            headers = new Headers();
        }

        Builder(Response response) {
            this.request = response.getRequest();
            this.headers = response.getHeaders();
            this.statusCode = response.getStatusCode();
            this.body = response.getBody();
            this.sentRequestAtMillis = response.getSentRequestAtMillis();
            this.receivedResponseAtMillis = response.getReceivedResponseAtMillis();
        }

        public Builder request(Request request) {
            this.request = request;
            return this;
        }

        public Builder headers(Headers headers) {
            this.headers = headers;
            return this;
        }

        public Builder statusCode(StatusCode statusCode) {
            this.statusCode = statusCode;
            return this;
        }

        public Builder body(ResponseBody body) {
            this.body = body;
            return this;
        }

        public Builder sentRequestAtMillis(long sentRequestAtMillis) {
            this.sentRequestAtMillis = sentRequestAtMillis;
            return this;
        }

        public Builder receivedResponseAtMillis(long receivedResponseAtMillis) {
            this.receivedResponseAtMillis = receivedResponseAtMillis;
            return this;
        }

        public Response build() {
            return new Response(this);
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

        Response response = (Response) o;

        if (sentRequestAtMillis != response.sentRequestAtMillis) {
            return false;
        }
        if (receivedResponseAtMillis != response.receivedResponseAtMillis) {
            return false;
        }
        if (request != null ? !request.equals(response.request) : response.request != null) {
            return false;
        }
        if (headers != null ? !headers.equals(response.headers) : response.headers != null) {
            return false;
        }
        if (statusCode != response.statusCode) {
            return false;
        }
        return body != null ? body.equals(response.body) : response.body == null;
    }

    @Override
    public int hashCode() {
        int result = request != null ? request.hashCode() : 0;
        result = 31 * result + (headers != null ? headers.hashCode() : 0);
        result = 31 * result + (statusCode != null ? statusCode.hashCode() : 0);
        result = 31 * result + (body != null ? body.hashCode() : 0);
        result = 31 * result + (int) (sentRequestAtMillis ^ (sentRequestAtMillis >>> 32));
        result = 31 * result + (int) (receivedResponseAtMillis ^ (receivedResponseAtMillis >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "Response{" +
                "request=" + request +
                ", headers=" + headers +
                ", statusCode=" + statusCode +
                ", body=" + body +
                ", sentRequestAtMillis=" + sentRequestAtMillis +
                ", receivedResponseAtMillis=" + receivedResponseAtMillis +
                '}';
    }
}
