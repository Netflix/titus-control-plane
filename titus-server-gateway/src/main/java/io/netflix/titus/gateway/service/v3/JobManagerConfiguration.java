package io.netflix.titus.gateway.service.v3;

import java.util.List;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titus.jobManager")
public interface JobManagerConfiguration {

    String getDefaultIamRole();

    List<String> getDefaultSecurityGroups();

    @DefaultValue("_none_")
    String getNoncompliantClientWhiteList();
}
