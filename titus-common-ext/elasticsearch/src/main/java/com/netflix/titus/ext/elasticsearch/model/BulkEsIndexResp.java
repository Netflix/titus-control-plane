package com.netflix.titus.ext.elasticsearch.model;

import java.util.List;

public class BulkEsIndexResp {
    List<BulkEsIndexRespItem> items;

    public List<BulkEsIndexRespItem> getItems() {
        return items;
    }

    public void setItems(List<BulkEsIndexRespItem> items) {
        this.items = items;
    }
}
