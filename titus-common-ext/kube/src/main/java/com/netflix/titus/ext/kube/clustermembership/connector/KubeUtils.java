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

package com.netflix.titus.ext.kube.clustermembership.connector;

import com.netflix.titus.api.clustermembership.connector.ClusterMembershipConnectorException;
import com.netflix.titus.common.util.ExceptionExt;
import io.kubernetes.client.openapi.ApiException;

public class KubeUtils {

    /**
     * Returns HTTP status code if found, or -1 otherwise.
     */
    public static int getHttpStatusCode(Throwable error) {
        Throwable cause = error;
        while (cause != null && !(cause instanceof ApiException)) {
            cause = cause.getCause();
        }
        return cause == null ? -1 : ((ApiException) cause).getCode();
    }

    public static boolean is4xx(Throwable error) {
        return getHttpStatusCode(error) / 100 == 4;
    }

    public static ClusterMembershipConnectorException toConnectorException(Throwable error) {
        if (!(error instanceof ApiException)) {
            return ClusterMembershipConnectorException.clientError(ExceptionExt.toMessageChain(error), error);
        }
        ApiException apiException = (ApiException) error;
        return ClusterMembershipConnectorException.clientError(
                String.format("%s: httpStatus=%s, body=%s", apiException.getMessage(), apiException.getCode(), apiException.getResponseBody()),
                error
        );
    }
}
