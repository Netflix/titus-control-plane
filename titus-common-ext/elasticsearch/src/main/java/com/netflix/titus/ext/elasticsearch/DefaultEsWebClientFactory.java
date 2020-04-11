/*
 * Copyright 2020 Netflix, Inc.
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

import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;
import reactor.netty.tcp.TcpClient;


public class DefaultEsWebClientFactory implements EsWebClientFactory {
    private EsClientConfiguration esClientConfiguration;

    public DefaultEsWebClientFactory(EsClientConfiguration esClientConfiguration) {
        this.esClientConfiguration = esClientConfiguration;
    }

    @Override
    public WebClient buildWebClient() {
        return WebClient.builder().clientConnector(new ReactorClientHttpConnector(buildHttpClient()))
                .baseUrl(buildEsUrl()).build();
    }


    @VisibleForTesting
    public String buildEsUrl() {
        return String.format("http://%s:%s", esClientConfiguration.getEsHostName(),
                esClientConfiguration.getEsPort());
    }


    private HttpClient buildHttpClient() {
        return HttpClient.create().tcpConfiguration(tcpClient -> {
            TcpClient tcpClientWithConnectionTimeout = tcpClient.option(ChannelOption.CONNECT_TIMEOUT_MILLIS,
                    esClientConfiguration.getConnectTimeoutMillis());
            return tcpClientWithConnectionTimeout.doOnConnected(connection -> {
                //TODO Investigate why WriteTimeoutHandler appears to be broken in netty-handler 4.1.36.RELEASE package.
                connection.addHandlerLast(new ReadTimeoutHandler(esClientConfiguration.getReadTimeoutSeconds()));
            });
        });
    }
}
