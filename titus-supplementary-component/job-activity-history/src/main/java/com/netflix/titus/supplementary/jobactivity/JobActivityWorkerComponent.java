package com.netflix.titus.supplementary.jobactivity;

import com.netflix.spectator.api.Registry;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.supplementary.jobactivity.store.JobActivityStore;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class JobActivityWorkerComponent {
    @Bean
    public JobActivityWorker getJobActivityWorker(TitusRuntime titusRuntime,
                                                  JobActivityStore jobActivityStore) {
        return new JobActivityWorker(titusRuntime, jobActivityStore);
    }
}
