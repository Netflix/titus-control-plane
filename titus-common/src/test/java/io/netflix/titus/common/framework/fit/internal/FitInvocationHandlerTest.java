package io.netflix.titus.common.framework.fit.internal;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.google.common.collect.ImmutableMap;
import io.netflix.titus.common.framework.fit.Fit;
import io.netflix.titus.common.framework.fit.FitComponent;
import io.netflix.titus.common.framework.fit.FitInjection;
import io.netflix.titus.common.framework.fit.internal.action.FitErrorAction;
import org.junit.Test;
import rx.Observable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class FitInvocationHandlerTest {

    private final FitInjection fitInjection = Fit.newFitInjectionBuilder("testInjection")
            .withExceptionType(MyException.class)
            .build();

    private final FitComponent fitComponent = Fit.newFitComponent("root").addInjection(fitInjection);

    private final MyApiImpl myApiImpl = new MyApiImpl();

    private final MyApi myApi = Fit.newFitProxy(myApiImpl, fitInjection);

    @Test
    public void testBeforeSynchronous() {
        configureAction(true);
        try {
            myApi.runSynchronous("hello");
            fail("Expected FIT injected error");
        } catch (MyException e) {
            assertThat(myApiImpl.getExecutionCounter()).isEqualTo(0);
        }

        assertThat(myApi.runSynchronous("hello")).isEqualTo("hello");
    }

    @Test
    public void testAfterSynchronous() {
        configureAction(false);
        try {
            myApi.runSynchronous("hello");
            fail("Expected FIT injected error");
        } catch (MyException e) {
            assertThat(myApiImpl.getExecutionCounter()).isEqualTo(1);
        }

        assertThat(myApi.runSynchronous("hello")).isEqualTo("hello");
    }

    @Test
    public void testBeforeCompletableFuture() throws Exception {
        configureAction(true);
        try {
            myApi.runCompletableFuture("hello").get();
            fail("Expected FIT injected error");
        } catch (ExecutionException e) {
            assertThat(e.getCause()).isInstanceOf(MyException.class);
            assertThat(myApiImpl.getExecutionCounter()).isEqualTo(0);
        }

        assertThat(myApi.runCompletableFuture("hello").get()).isEqualTo("hello");
    }

    @Test
    public void testAfterCompletableFuture() throws Exception {
        configureAction(false);
        try {
            myApi.runCompletableFuture("hello").get();
            fail("Expected FIT injected error");
        } catch (ExecutionException e) {
            assertThat(e.getCause()).isInstanceOf(MyException.class);
            assertThat(myApiImpl.getExecutionCounter()).isEqualTo(1);
        }

        assertThat(myApi.runCompletableFuture("hello").get()).isEqualTo("hello");
    }

    @Test
    public void testBeforeObservable() {
        configureAction(true);
        try {
            myApi.runObservable("hello").toBlocking().last();
            fail("Expected FIT injected error");
        } catch (MyException e) {
            assertThat(myApiImpl.getExecutionCounter()).isEqualTo(0);
        }

        assertThat(myApi.runObservable("hello").toBlocking().first()).isEqualTo("hello");
    }

    @Test
    public void testAfterObservable() {
        configureAction(false);
        try {
            myApi.runObservable("hello").toBlocking().last();
            fail("Expected FIT injected error");
        } catch (MyException e) {
            assertThat(myApiImpl.getExecutionCounter()).isEqualTo(1);
        }

        assertThat(myApi.runObservable("hello").toBlocking().first()).isEqualTo("hello");
    }

    private void configureAction(boolean before) {
        fitInjection.addAction(new FitErrorAction("errorAction", ImmutableMap.of(
                "percentage", "20",
                "before", Boolean.toString(before),
                "errorTime", "1m",
                "upTime", "0s"
        ), fitInjection));
    }

    public static class MyException extends RuntimeException {
        public MyException(String message) {
            super(message);
        }
    }

    public interface MyApi {
        String runSynchronous(String hello);

        CompletableFuture<String> runCompletableFuture(String arg);

        Observable<String> runObservable(String arg);
    }

    public static class MyApiImpl implements MyApi {

        private volatile int executionCounter;

        public int getExecutionCounter() {
            return executionCounter;
        }

        @Override
        public String runSynchronous(String arg) {
            executionCounter++;
            return arg;
        }

        @Override
        public CompletableFuture<String> runCompletableFuture(String arg) {
            executionCounter++;
            return CompletableFuture.completedFuture(arg);
        }

        @Override
        public Observable<String> runObservable(String arg) {
            return Observable.fromCallable(() -> {
                executionCounter++;
                return arg;
            });
        }
    }
}