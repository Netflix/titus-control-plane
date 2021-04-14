package com.netflix.titus.supplementary.jobactivity;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.common.framework.fit.FitComponent;
import com.netflix.titus.common.framework.fit.FitFramework;
import com.netflix.titus.common.framework.fit.adapter.GrpcFitInterceptor;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import org.springframework.context.annotation.Bean;

@Configuration
public class JobActivityTitusRuntimeComponent {
    private final TitusRuntime titusRuntime = TitusRuntimes.internal(true);

    @Bean
    public TitusRuntime getTitusRuntime() {
        FitFramework fitFramework = titusRuntime.getFitFramework();
        if (fitFramework.isActive()) {
            FitComponent root = fitFramework.getRootComponent();
            root.createChild(GrpcFitInterceptor.COMPONENT);
        }

        return titusRuntime;
    }

    @Bean
    public Registry getRegistry(TitusRuntime titusRuntime) {
        return titusRuntime.getRegistry();
    }
}
