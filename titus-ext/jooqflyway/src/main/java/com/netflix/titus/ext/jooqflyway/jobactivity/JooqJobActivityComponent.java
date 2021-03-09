package com.netflix.titus.ext.jooqflyway.jobactivity;


import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.ext.jooqflyway.jobactivity.JooqContext;
import com.netflix.titus.supplementary.jobactivity.store.JobActivityStore;
import org.jooq.DSLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JooqJobActivityComponent {

    @Bean
    public JobActivityStore getJobActivityStore(TitusRuntime titusRuntime,
                                                DSLContext jobActivityDSLContext,
                                                DSLContext producerDSLContext) {
        return new JooqJobActivityStore(titusRuntime, jobActivityDSLContext, producerDSLContext);
    }
}
