package com.netflix.titus.runtime.connector.eviction;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Configuration(prefix = "titus.eviction")
public interface EvictionConfiguration {
    /**
     * Pattern identifying application names that will be exempt from system disruption budget constraints
     */
    @DefaultValue("titusOps")
    String getAppsExemptFromSystemDisruptionBudget();
}


