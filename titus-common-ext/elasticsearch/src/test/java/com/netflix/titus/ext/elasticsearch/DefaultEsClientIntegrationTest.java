package com.netflix.titus.ext.elasticsearch;

import java.time.Instant;
import java.util.Arrays;

import com.netflix.titus.ext.elasticsearch.model.EsRespSrc;
import com.netflix.titus.testkit.junit.category.RemoteIntegrationTest;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.core.ParameterizedTypeReference;
import reactor.test.StepVerifier;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@Category(RemoteIntegrationTest.class)
public class DefaultEsClientIntegrationTest {
    @ClassRule
    public static final EsExternalResource ES_RESOURCE = new EsExternalResource();

    private DefaultEsClient<TestDoc> client;

    private static ParameterizedTypeReference<EsRespSrc<TestDoc>> esRespTypeRef = new ParameterizedTypeReference<EsRespSrc<TestDoc>>() {
    };

    @Before
    public void setup() {
        client = new DefaultEsClient<>(new DefaultEsWebClientFactory(ES_RESOURCE.getEsClientConfiguration()));
    }

    @Test
    public void indexDocument() {
        String index = "jobs";
        String type = "_doc";
        String docId = "foo-13";

        TestDoc testDoc = new TestDoc(docId, "Accepted", Instant.now().getEpochSecond());
        client.indexDocument(testDoc, index, type).block();

        EsRespSrc<TestDoc> respSrc = client.findDocumentById(docId, index, type, esRespTypeRef).block();
        assertThat(respSrc).isNotNull();
        assertThat(respSrc.get_source()).isNotNull();

        TestDoc testDocResp = respSrc.get_source();
        assertThat(testDocResp.getId()).isEqualTo(docId);
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

        TestDoc testDoc1 = new TestDoc(id1, id1State, Instant.now().getEpochSecond());
        TestDoc testDoc2 = new TestDoc(id2, id2State, Instant.now().getEpochSecond());
        TestDoc testDoc3 = new TestDoc(id3, id3State, Instant.now().getEpochSecond());

        StepVerifier.create(client.bulkIndexDocuments(Arrays.asList(testDoc1, testDoc2, testDoc3), index, type))
                .assertNext(bulkEsIndexResp -> {
                    assertThat(bulkEsIndexResp.getItems()).isNotNull();
                    assertThat(bulkEsIndexResp.getItems().size()).isGreaterThan(0);
                })
                .verifyComplete();

        StepVerifier.create(client.getTotalDocumentCount(index, type))
                .assertNext(esRespCount -> {
                    assertThat(esRespCount).isNotNull();
                    assertThat(esRespCount.getCount()).isGreaterThan(0);
                })
                .verifyComplete();

        StepVerifier.create(client.findDocumentById(id1, index, type, esRespTypeRef))
                .assertNext(testDocEsRespSrc -> {
                    assertThat(testDocEsRespSrc.get_source()).isNotNull();
                    assertThat(testDocEsRespSrc.get_source().getId()).isEqualTo(id1);
                    assertThat(testDocEsRespSrc.get_source().getState()).isEqualTo(id1State);
                })
                .verifyComplete();

        StepVerifier.create(client.findDocumentById(id2, index, type, esRespTypeRef))
                .assertNext(testDocEsRespSrc -> {
                    assertThat(testDocEsRespSrc.get_source()).isNotNull();
                    assertThat(testDocEsRespSrc.get_source().getId()).isEqualTo(id2);
                    assertThat(testDocEsRespSrc.get_source().getState()).isEqualTo(id2State);
                })
                .verifyComplete();

        StepVerifier.create(client.findDocumentById(id3, index, type, esRespTypeRef))
                .assertNext(testDocEsRespSrc -> {
                    assertThat(testDocEsRespSrc.get_source()).isNotNull();
                    assertThat(testDocEsRespSrc.get_source().getId()).isEqualTo(id3);
                    assertThat(testDocEsRespSrc.get_source().getState()).isEqualTo(id3State);
                })
                .verifyComplete();
    }
}