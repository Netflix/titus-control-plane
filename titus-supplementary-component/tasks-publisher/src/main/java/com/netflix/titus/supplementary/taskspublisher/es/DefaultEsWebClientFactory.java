/*
 * Copyright 2019 Netflix, Inc.
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
package com.netflix.titus.supplementary.taskspublisher.es;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.supplementary.taskspublisher.config.EsPublisherConfiguration;
import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;
import reactor.netty.tcp.TcpClient;


public class DefaultEsWebClientFactory implements EsWebClientFactory {

    public static final int READ_TIMEOUT_SECONDS = 20;

    private EsPublisherConfiguration esPublisherConfiguration;

    public DefaultEsWebClientFactory(EsPublisherConfiguration esPublisherConfiguration) {
        this.esPublisherConfiguration = esPublisherConfiguration;
    }

    @Override
    public WebClient buildWebClient() {
        return WebClient.builder().clientConnector(new ReactorClientHttpConnector(buildHttpClient()))
                .baseUrl(buildEsUrl()).build();
    }


    @VisibleForTesting
    String buildEsUrl() {
        return String.format("http://%s:%s", esPublisherConfiguration.getEsHostName(),
                esPublisherConfiguration.getEsPort());
    }


    private HttpClient buildHttpClient() {
        return HttpClient.create().tcpConfiguration(tcpClient -> {
            TcpClient tcpClientWithConnectionTimeout = tcpClient.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1000);
            return tcpClientWithConnectionTimeout.doOnConnected(connection -> {
                //TODO Investigate why WriteTimeoutHandler appears to be broken in netty-handler 4.1.36.RELEASE package.
                connection.addHandlerLast(new ReadTimeoutHandler(READ_TIMEOUT_SECONDS));
            });
        });
    }
}
