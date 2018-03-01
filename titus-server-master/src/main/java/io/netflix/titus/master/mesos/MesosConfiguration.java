package io.netflix.titus.master.mesos;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titus.mesos")
public interface MesosConfiguration {

    @DefaultValue("invalidRequest")
    String getInvalidRequestMessagePattern();

    @DefaultValue("crashed")
    String getCrashedMessagePattern();

    @DefaultValue("transientSystemError")
    String getTransientSystemErrorMessagePattern();

    @DefaultValue("localSystemError")
    String getLocalSystemErrorMessagePattern();

    @DefaultValue("unknownSystemError")
    String getUnknownSystemErrorMessagePattern();

    @DefaultValue("true")
    boolean isReconcilerEnabled();

    @DefaultValue("false")
    boolean isAllowReconcilerUpdatesForUnknownTasks();
}
