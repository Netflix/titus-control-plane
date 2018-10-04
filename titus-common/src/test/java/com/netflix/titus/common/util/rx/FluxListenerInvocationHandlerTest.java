package com.netflix.titus.common.util.rx;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import reactor.core.Disposable;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxListenerInvocationHandlerTest {

    private final EventObservable eventObservable = new EventObservable();

    @Test
    public void testListenerCallback() {
        Iterator it = ReactorExt.fromListener(EventListener.class, eventObservable::addListener, eventObservable::removeListener).toIterable().iterator();

        eventObservable.emit("A");
        assertThat(it.hasNext()).isTrue();
        assertThat(it.next()).isEqualTo("A");

        eventObservable.emit("B");
        assertThat(it.hasNext()).isTrue();
        assertThat(it.next()).isEqualTo("B");
    }

    @Test
    public void testCancellation() {
        Disposable subscription = ReactorExt.fromListener(EventListener.class, eventObservable::addListener, eventObservable::removeListener).subscribe();

        assertThat(eventObservable.eventHandler).isNotNull();
        subscription.dispose();
        assertThat(eventObservable.eventHandler).isNull();
    }

    @Test
    public void testErrorClosesSubscription() {
        RuntimeException simulatedError = new RuntimeException("Simulated error");
        AtomicReference<Throwable> errorRef = new AtomicReference<>();

        Disposable subscription = ReactorExt.fromListener(EventListener.class, eventObservable::addListener, eventObservable::removeListener).subscribe(
                next -> {
                    throw simulatedError;
                },
                errorRef::set
        );
        eventObservable.emit("A");
        assertThat(subscription.isDisposed()).isTrue();
        assertThat(errorRef.get()).isEqualTo(simulatedError);
    }

    private interface EventListener {
        void onEvent(String event);
    }

    private class EventObservable {

        private EventListener eventHandler;

        void addListener(EventListener eventHandler) {
            this.eventHandler = eventHandler;
        }

        void removeListener(EventListener eventHandler) {
            if (this.eventHandler == eventHandler) {
                this.eventHandler = null;
            }
        }

        void emit(String value) {
            if (eventHandler != null) {
                eventHandler.onEvent(value);
            }
        }
    }
}