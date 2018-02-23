package io.netflix.titus.common.framework.fit.internal;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import io.netflix.titus.common.framework.fit.FitAction;
import io.netflix.titus.common.framework.fit.FitActionDescriptor;
import io.netflix.titus.common.framework.fit.FitInjection;
import io.netflix.titus.common.framework.fit.FitRegistry;
import io.netflix.titus.common.util.tuple.Pair;

public class DefaultFitRegistry implements FitRegistry {

    private final List<FitActionDescriptor> actionDescriptors;
    private final Map<String, BiFunction<String, Map<String, String>, Function<FitInjection, FitAction>>> actionFactories;

    public DefaultFitRegistry(List<Pair<FitActionDescriptor, BiFunction<String, Map<String, String>, Function<FitInjection, FitAction>>>> actions) {
        this.actionDescriptors = actions.stream().map(Pair::getLeft).collect(Collectors.toList());
        this.actionFactories = actions.stream().collect(Collectors.toMap(p -> p.getLeft().getKind(), Pair::getRight));
    }


    @Override
    public List<FitActionDescriptor> getFitActionDescriptors() {
        return actionDescriptors;
    }

    @Override
    public Function<FitInjection, FitAction> newFitActionFactory(String actionKind, String id, Map<String, String> properties) {
        return Preconditions.checkNotNull(
                actionFactories.get(actionKind), "Action kind %s not found", actionKind
        ).apply(id, properties);
    }
}
