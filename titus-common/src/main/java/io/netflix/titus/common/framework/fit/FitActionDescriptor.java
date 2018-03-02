package io.netflix.titus.common.framework.fit;

import java.util.Map;

/**
 * Action metadata.
 */
public class FitActionDescriptor {

    private final String kind;
    private final String description;
    private final Map<String, String> configurableProperties;

    public FitActionDescriptor(String kind, String description, Map<String, String> configurableProperties) {
        this.kind = kind;
        this.description = description;
        this.configurableProperties = configurableProperties;
    }

    /**
     * Action type unique identifier.
     */
    public String getKind() {
        return kind;
    }

    /**
     * Human readable action description.
     */
    public String getDescription() {
        return description;
    }

    /**
     * Configuration properties accepted by the action.
     */
    public Map<String, String> getConfigurableProperties() {
        return configurableProperties;
    }
}
