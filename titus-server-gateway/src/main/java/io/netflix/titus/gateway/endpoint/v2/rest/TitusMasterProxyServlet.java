/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.gateway.endpoint.v2.rest;

import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.UriBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import io.netflix.titus.common.network.http.Headers;
import io.netflix.titus.common.network.http.HttpClient;
import io.netflix.titus.common.network.http.Methods;
import io.netflix.titus.common.network.http.Request;
import io.netflix.titus.common.network.http.RequestBody;
import io.netflix.titus.common.network.http.Response;
import io.netflix.titus.common.util.StringExt;
import io.netflix.titus.gateway.MetricConstants;
import io.netflix.titus.gateway.connector.titusmaster.Address;
import io.netflix.titus.gateway.connector.titusmaster.LeaderResolver;
import io.netflix.titus.gateway.connector.titusmaster.TitusMasterConnectorModule;
import io.netflix.titus.gateway.service.v2.LogUrlService;
import io.netflix.titus.gateway.startup.TitusGatewayConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class TitusMasterProxyServlet extends HttpServlet {

    private static final Logger logger = LoggerFactory.getLogger(TitusMasterProxyServlet.class);

    private static final Set<String> IGNORED_REQUEST_HEADERS = Sets.newHashSet("connection", "content-length", "date",
            "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailers", "transfer-encoding", "upgrade");

    private static final Set<String> IGNORED_RESPONSE_HEADERS = Sets.newHashSet("connection", "content-length", "date",
            "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailers", "transfer-encoding", "upgrade");

    private static final int MAX_BYTES_TO_BUFFER = 32_000;
    private static final String JOB = "JOB";
    private static final String TASK = "TASK";

    private static final String TITUS_HEADER_CALLER_HOST_ADDRESS = "X-Titus-CallerHostAddress";

    private final TitusGatewayConfiguration configuration;
    private final Registry registry;
    private final HttpClient httpClient;
    private final LeaderResolver leaderResolver;
    private final LogUrlService logUrlService;
    private final ObjectMapper objectMapper;
    private final Id baseId;

    @Inject
    public TitusMasterProxyServlet(TitusGatewayConfiguration configuration,
                                   Registry registry,
                                   @Named(TitusMasterConnectorModule.TITUS_MASTER_CLIENT) HttpClient httpClient,
                                   LeaderResolver leaderResolver,
                                   LogUrlService logUrlService) {
        this.configuration = configuration;
        this.registry = registry;
        this.httpClient = httpClient;
        this.leaderResolver = leaderResolver;
        this.logUrlService = logUrlService;
        this.objectMapper = new ObjectMapper();
        this.baseId = registry.createId(MetricConstants.METRIC_PROXY + "request");
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        proxyRequest(request, response);
    }

    @Override
    protected void doHead(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        proxyRequest(request, response);
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        proxyRequest(request, response);
    }

    @Override
    protected void doPut(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        proxyRequest(request, response);
    }

    @Override
    protected void doDelete(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        proxyRequest(request, response);
    }

    @Override
    protected void doOptions(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        proxyRequest(request, response);
    }

    @Override
    protected void doTrace(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        proxyRequest(request, response);
    }

    private void proxyRequest(HttpServletRequest request, HttpServletResponse response) throws IOException {
        if (!isAccessAllowed(request, response)) {
            return;
        }

        final long start = registry.clock().wallTime();
        try {
            doProxyRequest(request, response);
        } catch (URISyntaxException e) {
            logger.error("[PROXY ILLEGAL URI] Bad URI specified with error: ", e);
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        } catch (Exception e) {
            logger.error("[PROXY UNKNOWN ERROR] Unable to proxy request with error: ", e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        } finally {
            final long end = registry.clock().wallTime();
            registry.timer(createId(request.getMethod(), response.getStatus())).record(end - start, TimeUnit.MILLISECONDS);
        }
    }

    private void doProxyRequest(HttpServletRequest request, HttpServletResponse response) throws URISyntaxException, IOException {
        Optional<Address> leaderOptional = leaderResolver.resolve();
        if (!leaderOptional.isPresent()) {
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            return;
        }

        String titusMasterUri = leaderOptional.get().toString();
        String method = request.getMethod();
        String remoteIp = request.getRemoteAddr();
        URI requestUri = getServletRequestUri(request);
        String requestUriPath = requestUri.getPath();
        InputStream requestInputStream = configuration.isProxyErrorLoggingEnabled()
                ? new ByteCopyInputStream(request.getInputStream(), MAX_BYTES_TO_BUFFER) : request.getInputStream();

        if (method.equals(Methods.GET) && requestUriPath.contains("/logs/download/")) {
            handleLogsDownloadRequest(request, response);
            return;
        }

        if (method.equals(Methods.POST)) {
            if (requestUriPath.contains("/jobs/terminate/") || requestUriPath.contains("/jobs/kill/")) {
                String jobId = extractIdFromRequestUri(request.getRequestURI());
                if (!Strings.isNullOrEmpty(jobId)) {
                    handleKillRequest(JOB, jobId, titusMasterUri, request, response);
                    return;
                }
            }

            if (requestUriPath.contains("/tasks/terminate/") || requestUriPath.contains("/tasks/kill/")) {
                String taskId = extractIdFromRequestUri(request.getRequestURI());
                if (!Strings.isNullOrEmpty(taskId)) {
                    handleKillRequest(TASK, taskId, titusMasterUri, request, response);
                    return;
                }
            }
        }

        URI clientRequestUri = constructProxyUri(requestUri, titusMasterUri);
        Headers clientHeaders = getAllHeaders(request);

        Response clientResponse = null;
        InputStream responseInputStream = null;
        boolean exceptionOccurred = false;

        try {
            Request.Builder clientRequestBuilder = new Request.Builder()
                    .url(clientRequestUri.toString())
                    .method(method)
                    .headers(clientHeaders);

            if (requestInputStream != null && Methods.isBodyAllowed(method)) {
                clientRequestBuilder.body(RequestBody.create(requestInputStream));
            }

            clientResponse = httpClient.execute(clientRequestBuilder.build());
            response.setStatus(clientResponse.getStatusCode().getCode());
            Headers clientResponseHeaders = clientResponse.getHeaders();
            clientResponseHeaders.names().forEach(name -> {
                if (!IGNORED_RESPONSE_HEADERS.contains(name.toLowerCase())) {
                    clientResponseHeaders.values(name).forEach(value -> response.addHeader(name, value));
                }
            });

            if (clientResponse.hasBody()) {
                responseInputStream = clientResponse.getBody().get(InputStream.class);
                responseInputStream = (configuration.isProxyErrorLoggingEnabled() && !clientResponse.isSuccessful()) ?
                        new ByteCopyInputStream(responseInputStream, MAX_BYTES_TO_BUFFER) : responseInputStream;

                ByteStreams.copy(responseInputStream, response.getOutputStream());
            }
        } catch (Exception e) {
            exceptionOccurred = true;
            throw e;
        } finally {
            boolean logRequest = configuration.isProxyErrorLoggingEnabled() && (exceptionOccurred || (clientResponse != null && !clientResponse.isSuccessful()));
            boolean logResponse = configuration.isProxyErrorLoggingEnabled() && (clientResponse != null && !clientResponse.isSuccessful());
            String proxyErrorMessage = "";

            if (logRequest) {
                byte[] requestBodyBytes = new byte[0];
                if (requestInputStream instanceof ByteCopyInputStream) {
                    requestBodyBytes = ((ByteCopyInputStream) requestInputStream).getCopiedBytes();
                }
                int requestContentLength = requestBodyBytes.length;
                String requestBody = new String(requestBodyBytes);

                proxyErrorMessage = "\n[PROXY ERROR REQUEST] " + remoteIp + " " + method + " " + requestUri
                        + "\n\tHeaders: " + clientHeaders
                        + "\n\tContent-Length: " + requestContentLength;

                if (requestContentLength > 0) {
                    proxyErrorMessage += "\n\tBody: " + requestBody;
                }
            }

            if (logResponse) {
                byte[] responseBodyBytes = new byte[0];
                if (responseInputStream instanceof ByteCopyInputStream) {
                    responseBodyBytes = ((ByteCopyInputStream) responseInputStream).getCopiedBytes();
                }
                int responseContentLength = responseBodyBytes.length;

                String responseBody = new String(responseBodyBytes);

                proxyErrorMessage += "\n[PROXY ERROR RESPONSE] " + response.getStatus() + " " + method + " " + clientRequestUri
                        + "\n\tHeaders: " + clientResponse.getHeaders()
                        + "\n\tContent-Length: " + responseContentLength;

                if (responseContentLength > 0) {
                    proxyErrorMessage += "\n\tBody: " + responseBody;
                }

                if (clientResponse != null && clientResponse.hasBody()) {
                    clientResponse.getBody().close();
                }

                if (responseInputStream != null) {
                    responseInputStream.close();
                }
            }

            if (StringExt.isNotEmpty(proxyErrorMessage)) {
                logger.info(proxyErrorMessage);
            }
        }
    }

    private Headers getAllHeaders(HttpServletRequest request) {
        Headers clientHeaders = new Headers();
        Enumeration headerNames = request.getHeaderNames();
        while (headerNames.hasMoreElements()) {
            String key = (String) headerNames.nextElement();
            String value = request.getHeader(key);
            if (!IGNORED_REQUEST_HEADERS.contains(key.toLowerCase())) {
                clientHeaders.put(key, value);
            }
        }

        // Add Titus specific headers
        String upstreamCaller = request.getHeader(TITUS_HEADER_CALLER_HOST_ADDRESS);
        if (upstreamCaller != null) {
            clientHeaders.put(TITUS_HEADER_CALLER_HOST_ADDRESS, upstreamCaller + ',' + request.getRemoteHost());
        } else {
            clientHeaders.put(TITUS_HEADER_CALLER_HOST_ADDRESS, request.getRemoteHost());
        }

        return clientHeaders;
    }

    private URI getServletRequestUri(HttpServletRequest request) throws URISyntaxException {
        String uri = request.getRequestURL() + (request.getQueryString() != null ? "?" + request.getQueryString() : "");
        return new URI(uri);
    }

    private URI constructProxyUri(URI requestUri, String titusMasterUri) {

        URI masterUri = UriBuilder.fromUri(titusMasterUri).build();

        UriBuilder uriBuilder = UriBuilder.fromUri(requestUri)
                .scheme(masterUri.getScheme())
                .host(masterUri.getHost())
                .port(masterUri.getPort());

        return uriBuilder.build();
    }

    private void handleLogsDownloadRequest(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String requestUri = request.getRequestURI();
        final String taskId = extractIdFromRequestUri(requestUri);
        if (taskId.isEmpty()) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            String message = "A taskId was not specified in the uri: " + requestUri;
            response.getWriter().write(message);
        } else {
            Map<String, String> logUrls = logUrlService.getLogUrls(taskId);
            response.setStatus(HttpServletResponse.SC_OK);
            response.setHeader("Content-Type", "application/json");
            response.getWriter().write(objectMapper.writeValueAsString(logUrls));
        }
    }

    private void handleKillRequest(String entity, String id, String titusMasterUri, HttpServletRequest request, HttpServletResponse response) throws IOException, URISyntaxException {

        String method = request.getMethod();
        Headers headers = getAllHeaders(request);
        String remoteIp = request.getRemoteAddr();
        URI requestUri = getServletRequestUri(request);

        logger.info("[PROXY {} KILL REQUEST] {} {} {} {}", entity, method, requestUri, remoteIp, headers);

        String path = "";
        String content = "";
        if (entity.equals(JOB)) {
            path = "/api/v2/jobs/kill";
            content = String.format("{\"jobId\": \"%s\"}", id);
        } else if (entity.equals(TASK)) {
            path = "/api/v2/tasks/kill";
            content = String.format("{\"taskId\": \"%s\"}", id);
        }

        UriBuilder uriBuilder = UriBuilder.fromUri(titusMasterUri)
                .replacePath(path);

        URI clientRequestUri = uriBuilder.build();
        Response clientResponse = null;
        headers.set("Content-Type", "application/json");

        try {
            Request clientRequest = new Request.Builder()
                    .url(clientRequestUri.toString())
                    .method(method)
                    .headers(headers)
                    .body(RequestBody.create(content))
                    .build();

            clientResponse = httpClient.execute(clientRequest);
            response.setStatus(clientResponse.getStatusCode().getCode());
            Headers clientResponseHeaders = clientResponse.getHeaders();
            clientResponseHeaders.names().forEach(name -> {
                if (!IGNORED_RESPONSE_HEADERS.contains(name.toLowerCase())) {
                    clientResponseHeaders.values(name).forEach(value -> response.addHeader(name, value));
                }
            });
            String responseMessage = "";
            if (entity.equals("job")) {
                responseMessage = "The termination request has been accepted and the task will be put to terminating state.";
            } else if (entity.equals("task")) {
                responseMessage = String.format("[\"%s Killed\"]", id);
            }

            if (clientResponse.isSuccessful()) {
                response.getWriter().write(responseMessage);
            }

            logger.info("[PROXY {} KILL RESPONSE] {} {} {} {} {}", entity, clientResponse.getStatusCode().getCode(), method, clientRequestUri,
                    clientResponseHeaders, responseMessage);
        } finally {
            if (clientResponse != null && clientResponse.hasBody()) {
                clientResponse.getBody().close();
            }
        }
    }

    private String extractIdFromRequestUri(String requestUri) {
        final String[] parts = requestUri.split("/");
        if (parts.length > 0) {
            return parts[parts.length - 1];
        }
        return "";
    }

    private Id createId(String method, int statusCode) {
        String status = (statusCode / 100) + "xx";

        return baseId
                .withTag("method", method)
                .withTag("status", status)
                .withTag("statusCode", String.valueOf(statusCode));
    }

    private boolean isAccessAllowed(HttpServletRequest request, HttpServletResponse response) throws IOException {
        if (configuration.isV2Enabled()) {
            return true;
        }

        String path = request.getPathInfo();
        if (!path.contains("/jobs") && !path.contains("/tasks")) {
            return true;
        }

        response.setStatus(404);
        response.getOutputStream().print("V2 Engine is turned off");
        return false;
    }

    /**
     * Basic implementation of an InputStream wrapper that copies the first N read bytes so that
     * they can be read at a later time.
     */
    static class ByteCopyInputStream extends FilterInputStream {

        private int byteCopySize;
        private ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        private int position;

        public ByteCopyInputStream(InputStream inputStream, int byteCopySize) {
            super(inputStream);
            this.byteCopySize = byteCopySize;
        }

        @Override
        public synchronized int read() throws IOException {
            int byteRead = super.read();
            if (byteRead > -1 && position < byteCopySize) {
                bytes.write(byteRead);
                position++;
            }
            return byteRead;
        }

        @Override
        public synchronized int read(byte[] b, int off, int len) throws IOException {
            int bytesRead = super.read(b, off, len);
            if (bytesRead > 0) {
                int available = byteCopySize - position;
                if (available > 0) {
                    int length = (available < len) ? available : len;
                    bytes.write(b, off, length);
                    position += bytesRead;
                }
            }

            return bytesRead;
        }

        public byte[] getCopiedBytes() {
            if (position > 0) {
                return Arrays.copyOfRange(bytes.toByteArray(), 0, position + 1);
            }
            return new byte[0];
        }
    }
}
