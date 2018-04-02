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

package com.netflix.titus.runtime.endpoint.fit;

import com.netflix.titus.common.framework.fit.FitAction;
import com.netflix.titus.common.framework.fit.FitActionDescriptor;
import com.netflix.titus.common.framework.fit.FitComponent;
import com.netflix.titus.common.framework.fit.FitInjection;
import com.netflix.titus.runtime.Fit;

public final class ProtobufFitConverters {

    private ProtobufFitConverters() {
    }

    public static Fit.FitActionDescriptor toGrpcFitActionDescriptor(FitActionDescriptor fitActionDescriptor) {
        return Fit.FitActionDescriptor.newBuilder()
                .setKind(fitActionDescriptor.getKind())
                .setDescription(fitActionDescriptor.getDescription())
                .putAllConfigurableProperties(fitActionDescriptor.getConfigurableProperties())
                .build();
    }

    public static Fit.FitComponent toGrpcFitComponent(FitComponent coreFitComponent) {
        Fit.FitComponent.Builder builder = Fit.FitComponent.newBuilder()
                .setId(coreFitComponent.getId());

        coreFitComponent.getInjections().forEach(i -> builder.addInjections(toGrpcFitInjection(i)));
        coreFitComponent.getChildren().forEach(c -> builder.addChildren(toGrpcFitComponent(c)));

        return builder.build();
    }

    public static Fit.FitInjection toGrpcFitInjection(FitInjection coreFitInjection) {
        Fit.FitInjection.Builder builder = Fit.FitInjection.newBuilder()
                .setId(coreFitInjection.getId());

        coreFitInjection.getActions().forEach(a -> builder.addFitActions(toGrpcFitAction(a)));

        return builder.build();
    }

    public static Fit.FitAction toGrpcFitAction(FitAction coreFitAction) {
        return Fit.FitAction.newBuilder()
                .setId(coreFitAction.getId())
                .setActionKind(coreFitAction.getDescriptor().getKind())
                .putAllProperties(coreFitAction.getProperties())
                .build();
    }
}
