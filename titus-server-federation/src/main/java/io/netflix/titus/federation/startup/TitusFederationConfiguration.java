package io.netflix.titus.federation.startup;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titus.federation")
public interface TitusFederationConfiguration {

    @DefaultValue("cell1=hostName1:7001;cell2=hostName2:7002")
    String getCells();

    @DefaultValue("dev")
    String getStack();
}
