package io.netflix.titus.common.framework.fit;

import java.util.Map;

public class FitActionDescriptor {

    private final String kind;
    private final String description;
    private final Map<String, String> configurableProperties;

    public FitActionDescriptor(String kind, String description, Map<String, String> configurableProperties) {
        this.kind = kind;
        this.description = description;
        this.configurableProperties = configurableProperties;
    }

    public String getKind() {
        return kind;
    }

    public String getDescription() {
        return description;
    }

    public Map<String, String> getConfigurableProperties() {
        return configurableProperties;
    }
}
