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

package com.netflix.titus.master.mesos.kubeapiserver.direct;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import com.netflix.titus.common.framework.fit.AbstractFitAction;
import com.netflix.titus.common.framework.fit.FitActionDescriptor;
import com.netflix.titus.common.framework.fit.FitInjection;
import com.netflix.titus.common.framework.fit.FitUtil;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.StringExt;

public class KubeFitAction extends AbstractFitAction {

    public static final FitActionDescriptor DESCRIPTOR = new FitActionDescriptor(
            "kubeIntegration",
            "Inject failures in Kube integration",
            CollectionsExt.copyAndAdd(
                    FitUtil.PERIOD_ERROR_PROPERTIES,
                    "errorKinds", "A list of: " + StringExt.concatenate(ErrorKind.values(), ",")
            )
    );

    private final Set<ErrorKind> failurePoints;
    private final Supplier<Boolean> shouldFailFunction;

    public KubeFitAction(String id, Map<String, String> properties, FitInjection injection) {
        super(id, DESCRIPTOR, properties, injection);

        String errorKindsValue = properties.get("errorKinds");
        Preconditions.checkArgument(errorKindsValue != null, "Missing 'errorKinds' property");
        this.failurePoints = new HashSet<>(StringExt.parseEnumListIgnoreCase(errorKindsValue, ErrorKind.class));
        this.shouldFailFunction = FitUtil.periodicErrors(properties);
    }

    public enum ErrorKind {
        /**
         * Do not call KubeAPI, just fail the pod create request immediately.
         */
        POD_CREATE_ERROR,

        /**
         * Report an informer data to be stale.
         */
        INFORMER_NOT_SYNCED,
    }

    @Override
    public void beforeImmediate(String injectionPoint) {
        ErrorKind errorKind = ErrorKind.valueOf(injectionPoint);
        if (shouldFailFunction.get() && failurePoints.contains(errorKind)) {
            switch (errorKind) {
                case POD_CREATE_ERROR:
                    throw new IllegalStateException("Simulating pod create error");
                case INFORMER_NOT_SYNCED:
                    throw new IllegalStateException("Simulating informer data staleness");
            }
        }
    }
}
