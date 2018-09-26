package com.netflix.titus.common.network.reverseproxy.http;

import java.util.Optional;

import reactor.ipc.netty.http.client.HttpClient;

public interface ReactorHttpClientFactory {

    Optional<HttpClient> newHttpClient(String path);
}
