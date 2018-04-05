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

package com.netflix.titus.ext.elasticsearch;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import javax.inject.Named;
import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class ElasticsearchModule extends AbstractModule {

    public static final java.lang.String TASK_DOCUMENT_CONTEXT = "taskDocumentContext";

    @Override
    protected void configure() {
        bind(ElasticsearchTaskDocumentPublisher.class).asEagerSingleton();
    }

    @Provides
    @Singleton
    public ElasticsearchConfiguration getElasticsearchConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(ElasticsearchConfiguration.class);
    }

    @Provides
    @Singleton
    public Client getClient(ElasticsearchConfiguration configuration) throws UnknownHostException {
        Settings settings = Settings.settingsBuilder()
                .put("client.transport.ignore_cluster_name", true)
                .build();
        return TransportClient.builder().settings(settings).build().addTransportAddress(
                new InetSocketTransportAddress(InetAddress.getByName(configuration.getTaskDocumentEsHostName()), configuration.getTaskDocumentEsPort())
        );
    }

    @Provides
    @Singleton
    @Named(TASK_DOCUMENT_CONTEXT)
    public Map<String, String> getTaskDocumentContext() {
        return new HashMap<>();
    }
}
