package com.netflix.titus.ext.elasticsearch;

import java.time.Instant;
import java.util.Arrays;

import com.netflix.titus.ext.elasticsearch.model.EsRespSrc;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.ParameterizedTypeReference;
import reactor.test.StepVerifier;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class DefaultEsClientTest {
    public static class Payload implements EsDoc {
        String id;
        String state;
        long ts;

        public Payload() {
        }

        public void setId(String id) {
            this.id = id;
        }

        public void setState(String state) {
            this.state = state;
        }

        public void setTs(long ts) {
            this.ts = ts;
        }

        public Payload(String id, String state, long ts) {
            this.id = id;
            this.state = state;
            this.ts = ts;
        }

        public String getId() {
            return id;
        }

        public String getState() {
            return state;
        }

        public long getTs() {
            return ts;
        }
    }

    private DefaultEsClient<Payload> client;

    private EsClientConfiguration localEsClientConfiguration() {
        return new EsClientConfiguration() {
            @Override
            public int getReadTimeoutSeconds() {
                return 20;
            }

            @Override
            public int getConnectTimeoutMillis() {
                return 1000;
            }

            @Override
            public String getEsHostName() {
                return "localhost";
            }

            @Override
            public int getEsPort() {
                return 9200;
            }
        };
    }

    private static ParameterizedTypeReference<EsRespSrc<Payload>> esRespTypeRef = new ParameterizedTypeReference<EsRespSrc<Payload>>() {
    };

    @Before
    public void setup() {
        client = new DefaultEsClient<>(new DefaultEsWebClientFactory(localEsClientConfiguration()));
    }

    @Test
    public void indexDocument() {
        String index = "jobs";
        String type = "_doc";
        String docId = "foo-13";

        Payload payload = new Payload(docId, "Accepted", Instant.now().getEpochSecond());
        client.indexDocument(payload, index, type).block();

        EsRespSrc<Payload> respSrc = client.findDocumentById(docId, index, type, esRespTypeRef).block();
        assertThat(respSrc).isNotNull();
        assertThat(respSrc.get_source()).isNotNull();

        Payload payloadResp = respSrc.get_source();
        assertThat(payloadResp.getId()).isEqualTo(docId);
    }

    @Test
    public void bulkIndexDocument() {
        String index = "jobs";
        String type = "_doc";

        String id1 = "foo-100";
        String id2 = "foo-102";
        String id3 = "foo-104";

        String id1State = "Running";
        String id2State = "Starting";
        String id3State = "Queued";

        Payload payload1 = new Payload(id1, id1State, Instant.now().getEpochSecond());
        Payload payload2 = new Payload(id2, id2State, Instant.now().getEpochSecond());
        Payload payload3 = new Payload(id3, id3State, Instant.now().getEpochSecond());

        StepVerifier.create(client.bulkIndexDocument(Arrays.asList(payload1, payload2, payload3), index, type))
                .assertNext(bulkEsIndexResp -> {
                    assertThat(bulkEsIndexResp.items).isNotNull();
                    assertThat(bulkEsIndexResp.items.size()).isGreaterThan(0);
                })
                .verifyComplete();

        StepVerifier.create(client.getTotalDocumentCount(index, type))
                .assertNext(esRespCount -> {
                    assertThat(esRespCount).isNotNull();
                    assertThat(esRespCount.getCount()).isGreaterThan(0);
                })
                .verifyComplete();

        StepVerifier.create(client.findDocumentById(id1, index, type, esRespTypeRef))
                .assertNext(payloadEsRespSrc -> {
                    assertThat(payloadEsRespSrc.get_source()).isNotNull();
                    assertThat(payloadEsRespSrc.get_source().getId()).isEqualTo(id1);
                    assertThat(payloadEsRespSrc.get_source().getState()).isEqualTo(id1State);
                })
                .verifyComplete();

        StepVerifier.create(client.findDocumentById(id2, index, type, esRespTypeRef))
                .assertNext(payloadEsRespSrc -> {
                    assertThat(payloadEsRespSrc.get_source()).isNotNull();
                    assertThat(payloadEsRespSrc.get_source().getId()).isEqualTo(id2);
                    assertThat(payloadEsRespSrc.get_source().getState()).isEqualTo(id2State);
                })
                .verifyComplete();

        StepVerifier.create(client.findDocumentById(id3, index, type, esRespTypeRef))
                .assertNext(payloadEsRespSrc -> {
                    assertThat(payloadEsRespSrc.get_source()).isNotNull();
                    assertThat(payloadEsRespSrc.get_source().getId()).isEqualTo(id3);
                    assertThat(payloadEsRespSrc.get_source().getState()).isEqualTo(id3State);
                })
                .verifyComplete();
    }
}