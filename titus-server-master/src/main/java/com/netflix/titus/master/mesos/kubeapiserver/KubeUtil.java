/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.master.mesos.kubeapiserver;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.squareup.okhttp.Request;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.util.Config;

public class KubeUtil {

    public static final Pattern UUID_PATTERN = Pattern.compile("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");
    public static final Function<Request, String> DEFAULT_URI_MAPPER = r -> {
        String path = r.url().getPath();
        Matcher matcher = UUID_PATTERN.matcher(path);
        return matcher.replaceAll("");
    };

    public static CoreV1Api createApi(String serverUrl,
                                      String metricsNamePrefix,
                                      TitusRuntime titusRuntime,
                                      long readTimeoutMs) {
        ApiClient client = createApiClient(serverUrl, metricsNamePrefix, titusRuntime, readTimeoutMs);
        return new CoreV1Api(client);
    }

    public static ApiClient createApiClient(String serverUrl,
                                            String metricsNamePrefix,
                                            TitusRuntime titusRuntime,
                                            long readTimeoutMs) {
        return createApiClient(serverUrl, metricsNamePrefix, titusRuntime, DEFAULT_URI_MAPPER, readTimeoutMs);
    }

    public static ApiClient createApiClient(String serverUrl,
                                            String metricsNamePrefix,
                                            TitusRuntime titusRuntime,
                                            Function<Request, String> uriMapper,
                                            long readTimeoutMs) {
        OkHttpMetricsInterceptor metricsInterceptor = new OkHttpMetricsInterceptor(metricsNamePrefix, titusRuntime.getRegistry(),
                titusRuntime.getClock(), uriMapper);

        ApiClient client = Config.fromUrl(serverUrl);
        client.getHttpClient().setReadTimeout(readTimeoutMs, TimeUnit.MILLISECONDS);
        client.getHttpClient().interceptors().add(metricsInterceptor);
        return client;
    }

    public static SharedInformerFactory createSharedInformerFactory(String threadNamePrefix, ApiClient apiClient) {
        AtomicLong nextThreadNum = new AtomicLong(0);
        return new SharedInformerFactory(apiClient, Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, threadNamePrefix + nextThreadNum.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        }));
    }
}
