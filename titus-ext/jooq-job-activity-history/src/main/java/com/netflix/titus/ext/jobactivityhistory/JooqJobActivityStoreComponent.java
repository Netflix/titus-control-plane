package com.netflix.titus.ext.jobactivityhistory;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.ext.jooq.JooqContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import com.netflix.titus.supplementary.jobactivity.store.JobActivityStore;


@Configuration
public class JooqJobActivityStoreComponent {
    @Bean
    public JobActivityStore getJobActivityStore(TitusRuntime titusRuntime,
                                                JooqContext jobActivityJooqContext,
                                                JooqContext producerJooqContext) {
        return new JooqJobActivityStore(titusRuntime, jobActivityJooqContext, producerJooqContext);
    }
}
