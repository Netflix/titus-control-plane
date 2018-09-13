package com.netflix.titus.common.util.rx;

import java.util.function.Consumer;

import reactor.core.publisher.FluxSink;
import rx.Observable;
import rx.Subscription;

class FluxObservableEmitter<T> implements Consumer<FluxSink<T>> {

    private Observable<T> source;

    FluxObservableEmitter(Observable<T> source) {
        this.source = source;
    }

    @Override
    public void accept(FluxSink<T> sink) {
        Subscription subscription = source.subscribe(sink::next, sink::error, sink::complete);
        sink.onCancel(subscription::unsubscribe);
    }
}
