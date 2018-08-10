package com.netflix.titus.ext.zookeeper.mesos;

import com.google.inject.AbstractModule;
import com.netflix.titus.master.mesos.MesosMasterResolver;

public class ZookeeperMesosModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(MesosMasterResolver.class).to(ZkMesosMasterResolver.class);
    }
}
