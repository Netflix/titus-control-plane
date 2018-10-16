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

package com.netflix.titus.ext.eureka;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.annotation.Nullable;

import com.netflix.appinfo.ApplicationInfoManager;
import com.netflix.appinfo.HealthCheckCallback;
import com.netflix.appinfo.HealthCheckHandler;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.CacheRefreshedEvent;
import com.netflix.discovery.EurekaClient;
import com.netflix.discovery.EurekaClientConfig;
import com.netflix.discovery.EurekaEventListener;
import com.netflix.discovery.shared.Application;
import com.netflix.discovery.shared.Applications;

/**
 * A helper class for
 */
public class EurekaServerStub {

    private final ConcurrentMap<String, InstanceInfo> instanceInfos = new ConcurrentHashMap<>();

    private final EurekaClientStub eurekaClient = new EurekaClientStub();

    public EurekaClient getEurekaClient() {
        return eurekaClient;
    }

    public void register(InstanceInfo instanceInfo) {
        instanceInfos.put(instanceInfo.getId(), instanceInfo);
    }

    public void unregister(String instanceId) {
        instanceInfos.remove(instanceId);
    }

    public void triggerCacheRefreshUpdate() {
        eurekaClient.triggerCacheRefreshUpdate();
    }

    private class EurekaClientStub implements EurekaClient {

        private final List<EurekaEventListener> eventListeners = new CopyOnWriteArrayList<>();

        private void triggerCacheRefreshUpdate() {
            eventListeners.forEach(l -> l.onEvent(new CacheRefreshedEvent()));
        }

        @Override
        public Applications getApplicationsForARegion(@Nullable String region) {
            return null;
        }

        @Override
        public Applications getApplications(String serviceUrl) {
            return null;
        }

        @Override
        public List<InstanceInfo> getInstancesByVipAddress(String vipAddress, boolean secure) {
            return null;
        }

        @Override
        public List<InstanceInfo> getInstancesByVipAddress(String vipAddress, boolean secure, @Nullable String region) {
            return null;
        }

        @Override
        public List<InstanceInfo> getInstancesByVipAddressAndAppName(String vipAddress, String appName, boolean secure) {
            return null;
        }

        @Override
        public Set<String> getAllKnownRegions() {
            return null;
        }

        @Override
        public InstanceInfo.InstanceStatus getInstanceRemoteStatus() {
            return null;
        }

        @Override
        public List<String> getDiscoveryServiceUrls(String zone) {
            return null;
        }

        @Override
        public List<String> getServiceUrlsFromConfig(String instanceZone, boolean preferSameZone) {
            return null;
        }

        @Override
        public List<String> getServiceUrlsFromDNS(String instanceZone, boolean preferSameZone) {
            return null;
        }

        @Override
        public void registerHealthCheckCallback(HealthCheckCallback callback) {

        }

        @Override
        public void registerHealthCheck(HealthCheckHandler healthCheckHandler) {

        }

        @Override
        public void registerEventListener(EurekaEventListener eventListener) {
            eventListeners.add(eventListener);
        }

        @Override
        public boolean unregisterEventListener(EurekaEventListener eventListener) {
            return eventListeners.remove(eventListener);
        }

        @Override
        public HealthCheckHandler getHealthCheckHandler() {
            return null;
        }

        @Override
        public void shutdown() {
        }

        @Override
        public EurekaClientConfig getEurekaClientConfig() {
            return null;
        }

        @Override
        public ApplicationInfoManager getApplicationInfoManager() {
            return null;
        }

        @Override
        public Application getApplication(String appName) {
            return null;
        }

        @Override
        public Applications getApplications() {
            return null;
        }

        @Override
        public List<InstanceInfo> getInstancesById(String id) {
            InstanceInfo instanceInfo = instanceInfos.get(id);
            return instanceInfo == null ? Collections.emptyList() : Collections.singletonList(instanceInfo);
        }

        @Override
        public InstanceInfo getNextServerFromEureka(String virtualHostname, boolean secure) {
            return null;
        }
    }
}
