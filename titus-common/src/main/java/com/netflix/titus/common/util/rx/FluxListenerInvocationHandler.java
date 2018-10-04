package com.netflix.titus.common.util.rx;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.function.Consumer;

import reactor.core.publisher.Flux;

class FluxListenerInvocationHandler<T> implements InvocationHandler {

    private final Class<?> listenerType;
    private final Consumer<T> delegate;

    private FluxListenerInvocationHandler(Class<?> listenerType, Consumer<T> delegate) {
        this.listenerType = listenerType;
        this.delegate = delegate;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (method.getDeclaringClass().equals(listenerType)) {
            delegate.accept((T) args[0]);
            return null;
        }
        return method.invoke(this, args);
    }

    static <L, T> Flux<T> adapt(Class<L> listenerType, Consumer<L> register, Consumer<L> unregister) {
        return Flux.create(emitter -> {
            L callbackHandler = (L) Proxy.newProxyInstance(
                    listenerType.getClassLoader(),
                    new Class[]{listenerType},
                    new FluxListenerInvocationHandler<>(listenerType, emitter::next)
            );

            register.accept(callbackHandler);
            emitter.onDispose(() -> unregister.accept(callbackHandler));
        });
    }
}
