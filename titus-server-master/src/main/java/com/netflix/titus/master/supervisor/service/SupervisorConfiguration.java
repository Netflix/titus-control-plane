package com.netflix.titus.master.supervisor.service;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titusMaster.supervisor")
public interface SupervisorConfiguration {
    @DefaultValue("300000")
    long getBootstrapTimeoutMs();
}
