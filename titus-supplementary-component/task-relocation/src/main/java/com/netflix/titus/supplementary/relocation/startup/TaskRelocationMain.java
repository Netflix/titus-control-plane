package com.netflix.titus.supplementary.relocation.startup;

import com.netflix.archaius.guice.ArchaiusModule;
import com.netflix.governator.InjectorBuilder;
import com.netflix.governator.guice.jetty.Archaius2JettyModule;

public class TaskRelocationMain {

    public static void main(String[] args) throws Exception {
        InjectorBuilder.fromModules(
                new TaskRelocationModule(),
                new Archaius2JettyModule(),
                new ArchaiusModule() {
                    @Override
                    protected void configureArchaius() {
                        bindApplicationConfigurationOverrideResource("laptop");
                    }
                })
                .createInjector()
                .awaitTermination();
    }
}
