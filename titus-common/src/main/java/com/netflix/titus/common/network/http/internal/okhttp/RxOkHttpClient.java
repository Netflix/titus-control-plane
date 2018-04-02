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

package com.netflix.titus.common.network.http.internal.okhttp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;

import com.netflix.titus.common.network.http.Request;
import com.netflix.titus.common.network.http.RequestBody;
import com.netflix.titus.common.network.http.Response;
import com.netflix.titus.common.network.http.RxHttpClient;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.HttpUrl;
import okhttp3.Interceptor;
import rx.Emitter;
import rx.Observable;

public class RxOkHttpClient implements RxHttpClient {
    private static final String EMPTY_ENDPOINT = "http://<empty>";
    private static final int DEFAULT_CONNECT_TIMEOUT = 5_000;
    private static final int DEFAULT_READ_TIMEOUT = 5_000;
    private static final int DEFAULT_WRITE_TIMEOUT = 5_000;

    private final long connectTimeout;
    private final long readTimeout;
    private final long writeTimeout;
    private final SSLContext sslContext;
    private final X509TrustManager trustManager;
    private final List<Interceptor> interceptors;

    private okhttp3.OkHttpClient client;

    RxOkHttpClient(Builder builder) {
        this.connectTimeout = builder.connectTimeout;
        this.readTimeout = builder.readTimeout;
        this.writeTimeout = builder.writeTimeout;
        this.sslContext = builder.sslContext;
        this.trustManager = builder.trustManager;
        this.interceptors = builder.interceptors;

        okhttp3.OkHttpClient.Builder clientBuilder = new okhttp3.OkHttpClient.Builder()
                .connectTimeout(this.connectTimeout, TimeUnit.MILLISECONDS)
                .readTimeout(this.readTimeout, TimeUnit.MILLISECONDS)
                .writeTimeout(this.writeTimeout, TimeUnit.MILLISECONDS);

        if (sslContext != null && trustManager != null) {
            clientBuilder.sslSocketFactory(sslContext.getSocketFactory(), trustManager)
                    .hostnameVerifier((s, sslSession) -> true);
        }
        if (interceptors != null) {
            for (Interceptor interceptor : interceptors) {
                clientBuilder.addInterceptor(interceptor);
            }
        }

        this.client = clientBuilder.build();
    }

    @Override
    public Observable<Response> execute(Request request) {
        return Observable.create(emitter -> {
            Request newRequest = request;
            if (!request.getUrl().startsWith("http")) {
                newRequest = request.newBuilder()
                        .url(HttpUrl.parse(EMPTY_ENDPOINT + newRequest.getUrl()).toString())
                        .build();
            }

            okhttp3.Request okHttpRequest = OkHttpConverters.toOkHttpRequest(newRequest);

            client.newCall(okHttpRequest).enqueue(new Callback() {
                @Override
                public void onFailure(Call call, IOException e) {
                    emitter.onError(e);
                }

                @Override
                public void onResponse(Call call, okhttp3.Response response) throws IOException {
                    emitter.onNext(OkHttpConverters.fromOkHttpResponse(response));
                }
            });
        }, Emitter.BackpressureMode.NONE);
    }

    @Override
    public Observable<Response> get(String url) {
        Request request = new Request.Builder()
                .url(url)
                .get()
                .build();
        return execute(request);
    }

    @Override
    public Observable<Response> head(String url) {
        Request request = new Request.Builder()
                .url(url)
                .head()
                .build();
        return execute(request);
    }

    @Override
    public Observable<Response> post(String url, Object entity) {
        Request request = new Request.Builder()
                .url(url)
                .post()
                .body(RequestBody.create(entity))
                .build();
        return execute(request);
    }

    @Override
    public Observable<Response> delete(String url) {
        Request request = new Request.Builder()
                .url(url)
                .delete()
                .build();
        return execute(request);
    }

    @Override
    public Observable<Response> delete(String url, Object entity) {
        Request request = new Request.Builder()
                .url(url)
                .delete()
                .body(RequestBody.create(entity))
                .build();
        return execute(request);
    }

    @Override
    public Observable<Response> put(String url, Object entity) {
        Request request = new Request.Builder()
                .url(url)
                .put()
                .body(RequestBody.create(entity))
                .build();
        return execute(request);
    }

    @Override
    public Observable<Response> patch(String url, Object entity) {
        Request request = new Request.Builder()
                .url(url)
                .patch()
                .body(RequestBody.create(entity))
                .build();
        return execute(request);
    }

    public SSLContext sslContext() {
        return sslContext;
    }

    public X509TrustManager trustManager() {
        return trustManager;
    }

    public List<Interceptor> interceptors() {
        return interceptors;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(RxOkHttpClient rxOkHttpClient) {
        return new Builder(rxOkHttpClient);
    }

    public static class Builder {
        long connectTimeout = -1;
        long readTimeout = -1;
        long writeTimeout = -1;
        SSLContext sslContext;
        X509TrustManager trustManager;
        List<Interceptor> interceptors = new ArrayList<>();

        public Builder() {
        }

        Builder(RxOkHttpClient client) {
            this.connectTimeout = client.connectTimeout;
            this.readTimeout = client.readTimeout;
            this.writeTimeout = client.writeTimeout;
            this.sslContext = client.sslContext;
            this.trustManager = client.trustManager();
            this.interceptors = client.interceptors;
        }

        public Builder connectTimeout(long connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        public Builder readTimeout(long readTimeout) {
            this.readTimeout = readTimeout;
            return this;
        }

        public Builder writeTimeout(long writeTimeout) {
            this.writeTimeout = writeTimeout;
            return this;
        }

        public Builder sslContext(SSLContext sslContext) {
            this.sslContext = sslContext;
            return this;
        }

        public Builder trustManager(X509TrustManager trustManager) {
            this.trustManager = trustManager;
            return this;
        }

        public Builder interceptor(Interceptor interceptor) {
            this.interceptors.add(interceptor);
            return this;
        }

        public Builder interceptors(List<Interceptor> interceptors) {
            this.interceptors = interceptors;
            return this;
        }

        public RxOkHttpClient build() {
            if (connectTimeout < 0) {
                connectTimeout = DEFAULT_CONNECT_TIMEOUT;
            }
            if (readTimeout < 0) {
                readTimeout = DEFAULT_READ_TIMEOUT;
            }
            if (writeTimeout < 0) {
                writeTimeout = DEFAULT_WRITE_TIMEOUT;
            }
            return new RxOkHttpClient(this);
        }
    }
}
