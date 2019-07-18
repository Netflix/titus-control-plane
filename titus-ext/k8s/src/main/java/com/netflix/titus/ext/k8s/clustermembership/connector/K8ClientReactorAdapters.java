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

package com.netflix.titus.ext.k8s.clustermembership.connector;

import java.lang.reflect.Type;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.gson.reflect.TypeToken;
import com.netflix.titus.common.util.IOExt;
import com.netflix.titus.ext.k8s.clustermembership.connector.model.K8ClusterMembershipRevisionResource;
import com.netflix.titus.ext.k8s.clustermembership.connector.model.K8Status;
import com.squareup.okhttp.Call;
import io.kubernetes.client.ApiCallback;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.JSON;
import io.kubernetes.client.util.Watch;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class K8ClientReactorAdapters {

    private static final JSON K8_JSON = new JSON();

    private static final AtomicInteger WATCH_IDX = new AtomicInteger();

    static final Type TYPE_WATCH_CLUSTER_MEMBERSHIP_REVISION_RESOURCE = new TypeToken<Watch.Response<K8ClusterMembershipRevisionResource>>() {
    }.getType();

    interface CallProvider {
        Call newCall() throws ApiException;
    }

    interface CallProviderWithCallback {
        Call newCall(ApiCallback<Object> callback) throws ApiException;
    }

    static <T> Mono<K8Status> doGet(CallProviderWithCallback callProviderWithCallback, Class<T> type) {
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
                            K8Status k8Status = toK8Status(result);
                            Object spec = resultMap.get("spec");
                            if (spec != null) {
                                k8Status.spec(K8_JSON.deserialize(K8_JSON.serialize(spec), type));
                            }
                            sink.success(k8Status);
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

    static Mono<K8Status> doUpdate(CallProviderWithCallback callProviderWithCallback) {
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
                            sink.success(toK8Status(result));
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

    static <T> Flux<Watch.Response<T>> watch(ApiClient k8ApiClient, CallProvider callSupplier, Type watchType) {
        return Flux.create(sink -> {
            Watch<T> watch;
            try {
                watch = Watch.createWatch(k8ApiClient, callSupplier.newCall(), watchType);
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

    private static K8Status toK8Status(Object result) {
        return K8_JSON.deserialize(K8_JSON.serialize(result), K8Status.class);
    }
}
