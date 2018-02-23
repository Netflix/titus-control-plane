package io.netflix.titus.common.framework.fit;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public interface FitRegistry {

    List<FitActionDescriptor> getFitActionDescriptors();

    Function<FitInjection, FitAction> newFitActionFactory(String actionKind, String id, Map<String, String> properties);
}
