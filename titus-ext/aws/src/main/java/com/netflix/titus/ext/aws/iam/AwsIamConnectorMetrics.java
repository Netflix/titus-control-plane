/*
 *
 *  * Copyright 2019 Netflix, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.netflix.titus.ext.aws.iam;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import com.netflix.titus.common.util.spectator.ExecutionMetrics;

public class AwsIamConnectorMetrics {
    public enum AwsIamMethods {
        GetIamRole
    }

    private static final String METRICS_ROOT = "titus.iam.connector";

    private final Registry registry;
    private final Map<String, ExecutionMetrics> methodMetricsMap;

    public AwsIamConnectorMetrics(Registry registry) {
        this.registry = registry;
        methodMetricsMap = new ConcurrentHashMap<>();

        for (AwsIamMethods methodName : AwsIamMethods.values()) {
            // Create ExecutionMetrics that are preconfigured with the appropriate tags. This
            // allows latency metrics to be collected per method.
            List<Tag> tags = new ArrayList<>();
            String methodNameStr = methodName.name();
            tags.add(new BasicTag("method", methodNameStr));
            methodMetricsMap.put(methodNameStr,
                    new ExecutionMetrics(METRICS_ROOT, AwsIamConnector.class, registry, tags));
        }
    }

    public void success(AwsIamMethods method, long startTime) {
        getOrCreateMetrics(method).success(startTime);
    }

    public void failure(AwsIamMethods method, Throwable error, long startTime) {
        if (error.getMessage().contains("Rate exceeded")) {
            error = new AwsIamRateLimitException(error);
        }
        getOrCreateMetrics(method).failure(error, startTime);
    }

    // Creates an execution metric for the methodName if it doesn't exist. Returns the
    // metric if it exists already.
    private ExecutionMetrics getOrCreateMetrics(AwsIamMethods methodName) {
        String methodNameStr = methodName.name();
        if (methodMetricsMap.containsKey(methodNameStr)) {
            return methodMetricsMap.get(methodNameStr);
        }

        List<Tag> tags = new ArrayList<>();
        tags.add(new BasicTag("methodName", methodNameStr));
        ExecutionMetrics metric = new ExecutionMetrics(METRICS_ROOT, AwsIamConnector.class, registry, tags);

        methodMetricsMap.put(methodNameStr, metric);
        return metric;
    }
}
