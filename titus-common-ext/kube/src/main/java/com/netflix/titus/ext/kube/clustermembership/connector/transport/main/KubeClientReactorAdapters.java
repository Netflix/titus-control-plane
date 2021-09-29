/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.ext.kube.clustermembership.connector.transport.main;

import java.lang.reflect.Type;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.gson.reflect.TypeToken;
import com.netflix.titus.common.util.IOExt;
import com.netflix.titus.ext.kube.clustermembership.connector.transport.main.crd.KubeClusterMembershipRevisionResource;
import com.netflix.titus.ext.kube.clustermembership.connector.transport.main.crd.KubeStatus;
import io.kubernetes.client.openapi.ApiCallback;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.JSON;
import io.kubernetes.client.util.Watch;
import okhttp3.Call;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class KubeClientReactorAdapters {

    private static final JSON KUBE_JSON = new JSON();

    private static final AtomicInteger WATCH_IDX = new AtomicInteger();

    static final Type TYPE_WATCH_CLUSTER_MEMBERSHIP_REVISION_RESOURCE = new TypeToken<Watch.Response<KubeClusterMembershipRevisionResource>>() {
    }.getType();

    interface CallProvider {
        Call newCall() throws ApiException;
    }

    interface CallProviderWithCallback {
        Call newCall(ApiCallback<Object> callback) throws ApiException;
    }

    static <T> Mono<KubeStatus> doGet(CallProviderWithCallback callProviderWithCallback, Class<T> type) {
        return Mono.create(sink -> {
            Call call;
            try {
                call = callProviderWithCallback.newCall(new ApiCallback<Object>() {
                    @Override
                    public void onFailure(ApiException e, int statusCode, Map<String, List<String>> responseHeaders) {
                        sink.error(e);
                    }

                    @Override
                    public void onSuccess(Object result, int statusCode, Map<String, List<String>> responseHeaders) {
                        Map<String, Object> resultMap = (Map<String, Object>) result;
                        try {
                            KubeStatus kubeStatus = toKubeStatus(result);
                            Object spec = resultMap.get("spec");
                            if (spec != null) {
                                kubeStatus.spec(KUBE_JSON.deserialize(KUBE_JSON.serialize(spec), type));
                            }
                            sink.success(kubeStatus);
                        } catch (Exception e) {
                            sink.error(e);
                        }
                    }

                    @Override
                    public void onUploadProgress(long bytesWritten, long contentLength, boolean done) {
                    }

                    @Override
                    public void onDownloadProgress(long bytesRead, long contentLength, boolean done) {
                    }
                });

                sink.onCancel(call::cancel);

            } catch (Exception e) {
                sink.error(e);
            }
        });
    }

    static Mono<KubeStatus> doUpdate(CallProviderWithCallback callProviderWithCallback) {
        return Mono.create(sink -> {
            Call call;
            try {
                call = callProviderWithCallback.newCall(new ApiCallback<Object>() {
                    @Override
                    public void onFailure(ApiException e, int statusCode, Map<String, List<String>> responseHeaders) {
                        sink.error(e);
                    }

                    @Override
                    public void onSuccess(Object result, int statusCode, Map<String, List<String>> responseHeaders) {
                        try {
                            sink.success(toKubeStatus(result));
                        } catch (Exception e) {
                            sink.error(e);
                        }
                    }

                    @Override
                    public void onUploadProgress(long bytesWritten, long contentLength, boolean done) {
                    }

                    @Override
                    public void onDownloadProgress(long bytesRead, long contentLength, boolean done) {
                    }
                });

                sink.onCancel(call::cancel);

            } catch (Exception e) {
                sink.error(e);
            }
        });
    }

    static <T> Flux<Watch.Response<T>> watch(ApiClient kubeApiClient, CallProvider callSupplier, Type watchType) {
        return Flux.create(sink -> {
            Watch<T> watch;
            try {
                watch = Watch.createWatch(kubeApiClient, callSupplier.newCall(), watchType);
            } catch (ApiException e) {
                sink.error(e);
                return;
            }

            Thread watchThread = new Thread("WatchEventHandler-" + WATCH_IDX.getAndIncrement()) {
                @Override
                public void run() {
                    try {
                        for (Iterator<Watch.Response<T>> iterator = watch.iterator(); !sink.isCancelled() && iterator.hasNext(); ) {
                            Watch.Response<T> value = iterator.next();
                            sink.next(value);
                        }
                        sink.complete();
                    } catch (Throwable e) {
                        sink.error(e);
                    } finally {
                        IOExt.closeSilently(watch);
                    }
                }
            };
            watchThread.start();

            sink.onCancel(() -> IOExt.closeSilently(watch));
        });
    }

    private static KubeStatus toKubeStatus(Object result) {
        return KUBE_JSON.deserialize(KUBE_JSON.serialize(result), KubeStatus.class);
    }
}
