package com.netflix.titus.ext.elasticsearch.model;

import java.util.List;

public class EsRespHits<T> {
    List<EsRespSrc<T>> hits;

    public List<EsRespSrc<T>> getHits() {
        return hits;
    }

    public void setHits(List<EsRespSrc<T>> hits) {
        this.hits = hits;
    }

    @Override
    public String toString() {
        return "EsRespHits{" +
                "hits=" + hits +
                '}';
    }
}
