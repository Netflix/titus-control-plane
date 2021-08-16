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

package com.netflix.titus.runtime.connector.kubernetes;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import io.kubernetes.client.openapi.ApiCallback;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import okhttp3.Call;
import reactor.core.publisher.Mono;

public final class KubeUtil {

    /**
     * Like {@link Function}, but with {@link ApiException} throws clause.
     */
    public interface KubeFunction<I, O> {
        O apply(I argument) throws ApiException;
    }

    /**
     * Get Kube object name
     */
    public static String getMetadataName(V1ObjectMeta metadata) {
        if (metadata == null) {
            return "";
        }

        return metadata.getName();
    }

    public static <T> Mono<T> toReact(KubeFunction<ApiCallback<T>, Call> handler) {
        return Mono.create(sink -> {
            Call call;
            try {
                call = handler.apply(new ApiCallback<T>() {
                    @Override
                    public void onFailure(ApiException e, int statusCode, Map<String, List<String>> responseHeaders) {
                        sink.error(e);
                    }

                    @Override
                    public void onSuccess(T result, int statusCode, Map<String, List<String>> responseHeaders) {
                        if (result == null) {
                            sink.success();
                        } else {
                            sink.success(result);
                        }
                    }

                    @Override
                    public void onUploadProgress(long bytesWritten, long contentLength, boolean done) {
                    }

                    @Override
                    public void onDownloadProgress(long bytesRead, long contentLength, boolean done) {
                    }
                });
            } catch (ApiException e) {
                sink.error(e);
                return;
            }

            sink.onCancel(call::cancel);
        });
    }
}
