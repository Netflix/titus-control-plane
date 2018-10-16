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

package com.netflix.titus.common.network.reverseproxy.http;

import java.io.IOException;
import java.time.Duration;
import java.util.Enumeration;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.UriBuilder;

import com.google.common.io.ByteStreams;
import com.netflix.titus.common.util.tuple.Pair;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.http.client.HttpClient;
import reactor.ipc.netty.http.client.HttpClientRequest;
import reactor.ipc.netty.http.client.HttpClientResponse;

@Singleton
public class ReverseProxyServletFilter implements Filter {

    private static final Duration REQUEST_TIMEOUT = Duration.ofMillis(30_000);

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private final ReactorHttpClientFactory clientFactory;

    @Inject
    public ReverseProxyServletFilter(ReactorHttpClientFactory clientFactory) {
        this.clientFactory = clientFactory;
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) request;

        String serviceName = httpRequest.getRequestURI();
        Optional<HttpClient> httpClientOpt = clientFactory.newHttpClient(serviceName);
        if (httpClientOpt.isPresent()) {
            doForward(httpRequest, (HttpServletResponse) response, httpClientOpt.get());
        } else {
            chain.doFilter(request, response);
        }
    }

    private void doForward(HttpServletRequest request, HttpServletResponse response, HttpClient httpClient) {
        switch (request.getMethod()) {
            case "GET":
                doForwardGET(request, response, httpClient);
                break;
            case "POST":
                doForwardPOST(request, response, httpClient);
                break;
            case "PUT":
                doForwardPUT(request, response, httpClient);
                break;
            case "DELETE":
                doForwardDELETE(request, response, httpClient);
                break;
            default:
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        }
    }

    private void doForwardGET(HttpServletRequest clientRequest, HttpServletResponse clientResponse, HttpClient httpClient) {
        doExecute(
                clientResponse,
                httpClient.get(buildURI(clientRequest), proxyRequest -> copyHeaders(clientRequest, proxyRequest).send())
        );
    }

    private void doForwardPOST(HttpServletRequest clientRequest, HttpServletResponse clientResponse, HttpClient httpClient) {
        doExecute(
                clientResponse,
                httpClient.post(buildURI(clientRequest), proxyRequest -> copyHeaders(clientRequest, proxyRequest).send(readRequestBody(clientRequest)))
        );
    }

    private void doForwardPUT(HttpServletRequest clientRequest, HttpServletResponse clientResponse, HttpClient httpClient) {
        doExecute(
                clientResponse,
                httpClient.put(buildURI(clientRequest), proxyRequest -> copyHeaders(clientRequest, proxyRequest).send(readRequestBody(clientRequest)))
        );
    }

    private void doForwardDELETE(HttpServletRequest clientRequest, HttpServletResponse clientResponse, HttpClient httpClient) {
        doExecute(
                clientResponse,
                httpClient.delete(buildURI(clientRequest), proxyRequest -> copyHeaders(clientRequest, proxyRequest).send())
        );
    }

    @Override
    public void destroy() {
    }

    private String buildURI(HttpServletRequest request) {
        return UriBuilder.fromPath(request.getRequestURI())
                .replaceQuery(request.getQueryString())
                .build()
                .toString();
    }

    private HttpClientRequest copyHeaders(HttpServletRequest request, HttpClientRequest proxyRequest) {
        Enumeration<String> hIt = request.getHeaderNames();
        while (hIt.hasMoreElements()) {
            String name = hIt.nextElement();
            proxyRequest.addHeader(name, request.getHeader(name));
        }
        return proxyRequest;
    }

    private Publisher<ByteBuf> readRequestBody(HttpServletRequest clientRequest) {
        try {
            byte[] body = ByteStreams.toByteArray(clientRequest.getInputStream());
            return Mono.just(Unpooled.wrappedBuffer(body));
        } catch (Exception e) {
            return Mono.error(e);
        }
    }

    private void doExecute(HttpServletResponse response, Mono<HttpClientResponse> proxyResponse) {
        try {
            Pair<HttpClientResponse, byte[]> proxyResultPair = proxyResponse
                    .flatMap(this::readProxyResponseBody)
                    .block(REQUEST_TIMEOUT);
            sendProxyResponse(response, proxyResultPair.getLeft(), proxyResultPair.getRight());
        } catch (Exception e) {
            handleException(e, response);
        }
    }

    private Mono<Pair<HttpClientResponse, byte[]>> readProxyResponseBody(HttpClientResponse proxyResponse) {
        return proxyResponse.receive()
                .asByteArray()
                .last(EMPTY_BYTE_ARRAY)
                .map(body -> Pair.of(proxyResponse, body));
    }

    private void sendProxyResponse(HttpServletResponse response, HttpClientResponse proxyResponse, byte[] body) throws IOException {
        response.setStatus(proxyResponse.status().code());
        proxyResponse.responseHeaders().forEach(entry -> response.addHeader(entry.getKey(), entry.getValue()));
        response.getOutputStream().write(body);
    }

    private void handleException(Exception error, HttpServletResponse response) {
        response.setStatus(500);
    }
}
