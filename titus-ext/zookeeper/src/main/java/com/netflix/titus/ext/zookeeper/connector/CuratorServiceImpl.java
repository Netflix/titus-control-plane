/*
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.titus.ext.zookeeper.connector;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.spectator.api.Registry;
import com.netflix.titus.ext.zookeeper.ZookeeperConfiguration;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.GzipCompressionProvider;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This service implementation is responsible for managing the lifecycle of a {@link org.apache.curator.framework.CuratorFramework}
 * instance.
 */
@Singleton
public class CuratorServiceImpl implements CuratorService {

    private static final Logger LOG = LoggerFactory.getLogger(CuratorServiceImpl.class);

    private final CuratorFramework curator;
    private final AtomicInteger isConnectedGauge;

    @Inject
    public CuratorServiceImpl(ZookeeperConfiguration configs, ZookeeperClusterResolver clusterResolver, Registry registry) {
        isConnectedGauge = registry.gauge("titusMaster.curator.isConnected", new AtomicInteger());

        Optional<String> connectString = clusterResolver.resolve();
        if (!connectString.isPresent()) {
            // Fail early if connection to zookeeper not defined
            LOG.error("Zookeeper connectivity details not found");
            throw new IllegalStateException("Zookeeper connectivity details not found");
        }
        curator = CuratorFrameworkFactory.builder()
                .compressionProvider(new GzipCompressionProvider())
                .connectionTimeoutMs(configs.getZkConnectionTimeoutMs())
                .retryPolicy(new ExponentialBackoffRetry(configs.getZkConnectionRetrySleepMs(), configs.getZkConnectionMaxRetries()))
                .connectString(connectString.get())
                .build();
    }

    private void setupCuratorListener() {
        LOG.info("Setting up curator state change listener");
        curator.getConnectionStateListenable().addListener(new ConnectionStateListener() {
            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                if (newState.isConnected()) {
                    LOG.info("Curator connected");
                    isConnectedGauge.set(1);
                } else {
                    // ToDo: determine if it is safe to restart our service instead of committing suicide
                    LOG.error("Curator connection lost");
                    isConnectedGauge.set(0);
                }
            }
        });
    }

    @PostConstruct
    public void start() throws Exception {
        isConnectedGauge.set(0);
        setupCuratorListener();
        curator.start();
    }

    @PreDestroy
    public void shutdown() {
        try {
            curator.close();
        } catch (Exception e) {
            // A shutdown failure should not affect the subsequent shutdowns, so we just warn here
            LOG.warn("Failed to shut down the curator service: {}", e.getMessage(), e);
        }
    }

    public CuratorFramework getCurator() {
        return curator;
    }
}
