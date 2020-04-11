package com.netflix.titus.ext.elasticsearch;

public class TestDoc implements EsDoc {
    String id;
    String state;
    long ts;

    public TestDoc() {
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

    public TestDoc(String id, String state, long ts) {
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
