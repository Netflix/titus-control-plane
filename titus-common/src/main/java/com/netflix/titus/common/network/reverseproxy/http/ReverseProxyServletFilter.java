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
import java.io.OutputStream;
import java.time.Duration;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.UriBuilder;

import com.google.common.io.ByteStreams;
import com.netflix.titus.common.util.IOExt;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpHeaders;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.HttpClientResponse;

@Singleton
public class ReverseProxyServletFilter implements Filter {

    private static final Duration REQUEST_TIMEOUT = Duration.ofMillis(30_000);

    private final ReactorHttpClientFactory clientFactory;

    @Inject
    public ReverseProxyServletFilter(ReactorHttpClientFactory clientFactory) {
        this.clientFactory = clientFactory;
    }

    @Override
    public void init(FilterConfig filterConfig) {
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
        HttpClient.ResponseReceiver<?> request = httpClient
                .headers(headers -> copyHeaders(clientRequest, headers))
                .get()
                .uri(buildURI(clientRequest));
        replyHandler(request, clientResponse);
    }

    private void doForwardPOST(HttpServletRequest clientRequest, HttpServletResponse clientResponse, HttpClient httpClient) {
        HttpClient.ResponseReceiver<?> request = httpClient
                .headers(headers -> copyHeaders(clientRequest, headers))
                .post()
                .uri(buildURI(clientRequest))
                .send(readRequestBody(clientRequest));
        replyHandler(request, clientResponse);
    }

    private void doForwardPUT(HttpServletRequest clientRequest, HttpServletResponse clientResponse, HttpClient httpClient) {
        HttpClient.ResponseReceiver<?> request = httpClient
                .headers(headers -> copyHeaders(clientRequest, headers))
                .put()
                .uri(buildURI(clientRequest))
                .send(readRequestBody(clientRequest));
        replyHandler(request, clientResponse);
    }

    private void doForwardDELETE(HttpServletRequest clientRequest, HttpServletResponse clientResponse, HttpClient httpClient) {
        HttpClient.ResponseReceiver<?> request = httpClient
                .headers(headers -> copyHeaders(clientRequest, headers))
                .delete()
                .uri(buildURI(clientRequest))
                .send(readRequestBody(clientRequest));
        replyHandler(request, clientResponse);
    }

    private void replyHandler(HttpClient.ResponseReceiver<?> request, HttpServletResponse clientResponse) {
        Iterator<Object> outputIt = request
                .response((httpResponse, bodyStream) -> Flux.<Object>just(httpResponse).concatWith(bodyStream.map(this::toByteArray)))
                .timeout(REQUEST_TIMEOUT)
                .toIterable()
                .iterator();
        HttpClientResponse httpResponse = (HttpClientResponse) outputIt.next();
        clientResponse.setStatus(httpResponse.status().code());
        httpResponse.responseHeaders().forEach(entry -> clientResponse.addHeader(entry.getKey(), entry.getValue()));
        try (OutputStream bodyOS = clientResponse.getOutputStream()) {
            while (outputIt.hasNext()) {
                byte[] bodyPart = (byte[]) outputIt.next();
                bodyOS.write(bodyPart);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Output stream error", e);
        }
    }

    private byte[] toByteArray(ByteBuf byteBuf) {
        byte[] buf = new byte[byteBuf.readableBytes()];
        byteBuf.readBytes(buf);
        return buf;
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

    private void copyHeaders(HttpServletRequest request, HttpHeaders proxyRequestHeaders) {
        Enumeration<String> hIt = request.getHeaderNames();
        while (hIt.hasMoreElements()) {
            String name = hIt.nextElement();
            proxyRequestHeaders.add(name, request.getHeader(name));
        }
    }

    private Publisher<ByteBuf> readRequestBody(HttpServletRequest clientRequest) {
        ServletInputStream inputStream = null;
        try {
            inputStream = clientRequest.getInputStream();
            byte[] body = ByteStreams.toByteArray(inputStream);
            return Mono.just(Unpooled.wrappedBuffer(body));
        } catch (Exception e) {
            return Mono.error(e);
        } finally {
            IOExt.closeSilently(inputStream);
        }
    }
}
