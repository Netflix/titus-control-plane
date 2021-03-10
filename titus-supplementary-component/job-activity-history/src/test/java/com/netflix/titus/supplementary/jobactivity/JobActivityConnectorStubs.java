package com.netflix.titus.supplementary.jobactivity;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.StaticApplicationContext;

public class JobActivityConnectorStubs {

    private final TitusRuntime titusRuntime;
    private final AnnotationConfigApplicationContext container;

    public JobActivityConnectorStubs() {
        this(TitusRuntimes.test());
    }

    public JobActivityConnectorStubs(TitusRuntime titusRuntime) {
        //MockEnvironment config = new MockEnvironment();

        this.titusRuntime = titusRuntime;
        this.container = new AnnotationConfigApplicationContext();
        //container.getEnvironment().merge(config);

        container.setParent(getApplicationContext());
        container.refresh();
        container.start();
    }

    public ApplicationContext getApplicationContext() {
        StaticApplicationContext context = new StaticApplicationContext();
        context.getBeanFactory().registerSingleton("titusRuntime", titusRuntime);
        context.refresh();
        return context;
    }

    public TitusRuntime getTitusRuntime() {
        return titusRuntime;
    }

    public void shutdown() {
        container.close();
    }

}
