package com.netflix.titus.ext.elasticsearch.model;

/**
 * Elastic search data model as defined by search API documentation
 * https://www.elastic.co/guide/en/elasticsearch/reference/5.6/search.html
 */
public class EsSearchResp<T> {
    EsRespHits<T> hits;

    public EsRespHits<T> getHits() {
        return hits;
    }

    public void setHits(EsRespHits<T> hits) {
        this.hits = hits;
    }

    @Override
    public String toString() {
        return "EsSearchResp{" +
                "hits=" + hits +
                '}';
    }
}
