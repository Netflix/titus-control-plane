package com.netflix.titus.ext.elasticsearch.model;

public class BulkEsIndexRespItem {
    EsIndexResp index;

    public EsIndexResp getIndex() {
        return index;
    }

    public void setIndex(EsIndexResp index) {
        this.index = index;
    }
}
