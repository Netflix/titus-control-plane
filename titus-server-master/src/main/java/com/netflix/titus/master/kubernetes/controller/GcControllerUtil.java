/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.master.kubernetes.controller;

import com.google.gson.JsonSyntaxException;
import com.netflix.titus.master.kubernetes.KubeUtil;
import com.netflix.titus.runtime.connector.kubernetes.KubeApiException;
import com.netflix.titus.runtime.connector.kubernetes.std.StdKubeApiFacade;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1Pod;
import org.slf4j.Logger;

import static com.netflix.titus.runtime.kubernetes.KubeConstants.DEFAULT_NAMESPACE;

public final class GcControllerUtil {

    public static boolean deleteNode(StdKubeApiFacade kubeApiFacade, Logger logger, V1Node node) {
        String nodeName = KubeUtil.getMetadataName(node.getMetadata());
        try {
            kubeApiFacade.deleteNode(nodeName);
            return true;
        } catch (JsonSyntaxException e) {
            // this will be counted as successful as the response type mapping in the client is incorrect
            return true;
        } catch (KubeApiException e) {
            if (e.getErrorCode() != KubeApiException.ErrorCode.NOT_FOUND) {
                logger.error("Failed to delete node: {} with error: ", nodeName, e);
            }
        } catch (Exception e) {
            logger.error("Failed to delete node: {} with error: ", nodeName, e);
        }
        return false;
    }

    public static boolean deletePod(StdKubeApiFacade kubeApiFacade, Logger logger, V1Pod pod) {
        String podName = KubeUtil.getMetadataName(pod.getMetadata());
        try {
            kubeApiFacade.deleteNamespacedPod(DEFAULT_NAMESPACE, podName);
            return true;
        } catch (JsonSyntaxException e) {
            // this will be counted as successful as the response type mapping in the client is incorrect
            return true;
        } catch (KubeApiException e) {
            if (e.getErrorCode() != KubeApiException.ErrorCode.NOT_FOUND) {
                logger.error("Failed to delete pod: {} with error: ", podName, e);
            }
        } catch (Exception e) {
            logger.error("Failed to delete pod: {} with error: ", podName, e);
        }
        return false;
    }

}
