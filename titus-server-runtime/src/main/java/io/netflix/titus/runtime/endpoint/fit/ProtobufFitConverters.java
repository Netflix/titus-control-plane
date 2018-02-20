package io.netflix.titus.runtime.endpoint.fit;

import com.netflix.titus.runtime.Fit;
import io.netflix.titus.common.framework.fit.FitAction;
import io.netflix.titus.common.framework.fit.FitActionDescriptor;
import io.netflix.titus.common.framework.fit.FitComponent;
import io.netflix.titus.common.framework.fit.FitInjection;

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
                .setActionDescriptor(toGrpcFitActionDescriptor(coreFitAction.getDescriptor()))
                .putAllProperties(coreFitAction.getProperties())
                .build();
    }
}
