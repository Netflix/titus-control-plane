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

import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Optional;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

import com.netflix.titus.runtime.endpoint.common.rest.JsonMessageReaderWriter;
import com.netflix.titus.runtime.endpoint.common.rest.TitusExceptionMapper;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.jaxrs.JaxRsServerResource;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpContent;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.ipc.netty.http.client.HttpClient;
import reactor.ipc.netty.http.client.HttpClientResponse;

import static org.assertj.core.api.Assertions.assertThat;

@Category(IntegrationTest.class)
public class ReverseProxyServletFilterTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private static final String X_TITUS_REMOVED_KEY = "X-Titus-RemovedKey";

    @ClassRule
    public static final JaxRsServerResource<ForwardedResource> mainServer = JaxRsServerResource.newBuilder(new ForwardedResource())
            .withProviders(new JsonMessageReaderWriter(), new TitusExceptionMapper())
            .build();

    private static final ReactorHttpClientFactory reactorClientFactory = serviceName -> {
        if (serviceName.contains("/forwarded")) {
            return Optional.of(HttpClient.create("localhost", mainServer.getPort()));
        }
        return Optional.empty();
    };

    @ClassRule
    public static final JaxRsServerResource<NotForwardedResource> proxyServer = JaxRsServerResource.newBuilder(new NotForwardedResource())
            .withProviders(new JsonMessageReaderWriter(), new TitusExceptionMapper())
            .withFilter(new ReverseProxyServletFilter(reactorClientFactory))
            .build();

    @Test
    public void testGetWithoutForwarding() {
        HttpClientResponse response = HttpClient.create("localhost", proxyServer.getPort()).get("/ROOT/notForwarded/samples?value=hello").block(TIMEOUT);
        assertThat(response.status().code()).isEqualTo(200);
        assertThat(response.receiveContent().map(this::httpContentToString).blockFirst(TIMEOUT)).isEqualTo("\"notForwarded:hello\"");
    }

    @Test
    public void testGetWithForwarding() {
        HttpClientResponse response = HttpClient.create("localhost", proxyServer.getPort()).get("/ROOT/forwarded/samples?value=hello").block(TIMEOUT);
        assertThat(response.status().code()).isEqualTo(200);
        assertThat(response.receiveContent().map(this::httpContentToString).blockFirst(TIMEOUT)).isEqualTo("\"forwarded:hello\"");
    }

    @Test
    public void testPostWithoutForwarding() {
        HttpClientResponse response = HttpClient.create("localhost", proxyServer.getPort())
                .post("/ROOT/notForwarded/samples", r -> r.send(toFluxByteBuf("Echo123")))
                .block(TIMEOUT);
        assertThat(response.status().code()).isEqualTo(200);
        assertThat(response.receiveContent().map(this::httpContentToString).blockFirst(TIMEOUT)).isEqualTo("\"notForwarded:Echo123\"");
    }

    @Test
    public void testPostWithForwarding() {
        HttpClientResponse response = HttpClient.create("localhost", proxyServer.getPort())
                .post("/ROOT/forwarded/samples", r -> r.send(toFluxByteBuf("Echo123")))
                .block(TIMEOUT);
        assertThat(response.status().code()).isEqualTo(200);
        assertThat(response.receiveContent().map(this::httpContentToString).blockFirst(TIMEOUT)).isEqualTo("\"forwarded:Echo123\"");
    }

    @Test
    public void testDeleteWithoutForwarding() {
        HttpClientResponse response = HttpClient.create("localhost", proxyServer.getPort())
                .delete("/ROOT/notForwarded/samples/abc")
                .block(TIMEOUT);
        assertThat(response.status().code()).isEqualTo(204);
    }

    @Test
    public void testDeleteWithForwarding() {
        HttpClientResponse response = HttpClient.create("localhost", proxyServer.getPort())
                .delete("/ROOT/forwarded/samples/abc")
                .block(TIMEOUT);
        assertThat(response.status().code()).isEqualTo(204);
        assertThat(response.responseHeaders().get(X_TITUS_REMOVED_KEY)).isEqualTo("forwarded:abc");
    }

    private Publisher<? extends ByteBuf> toFluxByteBuf(String value) {
        return Flux.just(Unpooled.wrappedBuffer(value.getBytes()));
    }

    private String httpContentToString(HttpContent bufHolder) {
        ByteBuf bodyBuf = bufHolder.content();
        byte[] bytes = new byte[bodyBuf.readableBytes()];
        bodyBuf.getBytes(bodyBuf.readerIndex(), bytes);
        return new String(bytes, Charset.defaultCharset());
    }

    @Path("/ROOT")
    public static class NotForwardedResource {
        @Path("/notForwarded/samples")
        @GET
        public String doGET(@QueryParam("value") String value) {
            return "notForwarded:" + value;
        }

        @Path("/notForwarded/samples")
        @POST
        public String doPOST(String value) {
            return "notForwarded:" + value;
        }

        @Path("/notForwarded/samples/{key}")
        @DELETE
        public Response doDELETE(@PathParam("key") String key) {
            return Response.status(204).header(X_TITUS_REMOVED_KEY, "notForwarded:" + key).build();
        }
    }

    @Path("/ROOT")
    public static class ForwardedResource {
        @Path("/forwarded/samples")
        @GET
        public String doGET(@QueryParam("value") String value) {
            return "forwarded:" + value;
        }

        @Path("/forwarded/samples")
        @POST
        public String doPOST(String value) {
            return "forwarded:" + value;
        }

        @Path("/forwarded/samples/{key}")
        @DELETE
        public Response doDELETE(@PathParam("key") String key) {
            return Response.status(204).header(X_TITUS_REMOVED_KEY, "forwarded:" + key).build();
        }
    }
}