package com.netflix.titus.ext.jooqflyway.jobactivity;


import com.netflix.titus.common.runtime.TitusRuntime;
import org.jooq.DSLContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JooqJobActivityComponent {
    @Bean
    public JooqJobActivityStore getJobActivityStore(TitusRuntime titusRuntime,
                                                    DSLContext jobActivityDSLContext,
                                                    DSLContext producerDSLContext) {
        return new JooqJobActivityStore(titusRuntime, jobActivityDSLContext, producerDSLContext);
    }
}
