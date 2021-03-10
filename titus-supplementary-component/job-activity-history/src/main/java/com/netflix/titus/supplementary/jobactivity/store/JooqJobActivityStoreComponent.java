package com.netflix.titus.supplementary.jobactivity.store;

import com.netflix.titus.common.runtime.TitusRuntime;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JooqJobActivityStoreComponent {
    @Bean
    public JobActivityStore getJobActivityStore(TitusRuntime titusRuntime,
                                                JooqContext jobActivityJooqContext,
                                                JooqContext producerJooqContext) {
        return new JooqJobActivityStore(titusRuntime, jobActivityJooqContext, producerJooqContext);
    }
}
