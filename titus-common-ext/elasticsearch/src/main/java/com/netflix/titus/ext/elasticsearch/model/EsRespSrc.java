package com.netflix.titus.ext.elasticsearch.model;

public class EsRespSrc<T> {
    T _source;

    public T get_source() {
        return _source;
    }

    public void set_source(T _source) {
        this._source = _source;
    }

    @Override
    public String toString() {
        return "EsRespSrc{" +
                "_source=" + _source +
                '}';
    }
}
