package io.netflix.titus.common.framework.fit;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * A container for FIT action kinds.
 */
public interface FitRegistry {

    /**
     * Returns list of all known action descriptors.
     */
    List<FitActionDescriptor> getFitActionDescriptors();

    /**
     * Returns a FIT action factory for the provided configuration data.
     * <h1>Why FIT action is not created immediately?</h1>
     * The factory method is returned, as to finish the action construction process the {@link FitInjection} instance
     * is needed, and it is not always available at this stage.
     */
    Function<FitInjection, FitAction> newFitActionFactory(String actionKind, String id, Map<String, String> properties);
}
