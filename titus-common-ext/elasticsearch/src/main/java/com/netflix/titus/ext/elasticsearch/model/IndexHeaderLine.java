package com.netflix.titus.ext.elasticsearch.model;

import com.netflix.titus.ext.elasticsearch.IndexHeader;

public class IndexHeaderLine {
    private IndexHeader index;

    public IndexHeader getIndex() {
        return index;
    }

    public void setIndex(IndexHeader index) {
        this.index = index;
    }
}

