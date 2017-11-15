package io.netflix.titus.runtime.endpoint.resolver;

import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;

/**
 * Caller resolver that uses Eureka information.
 */
@Singleton
public class TitusHeaderHttpCallerIdResolver implements HttpCallerIdResolver {

    private static final String TITUS_HEADER_CALLER_HOST_ADDRESS = "X-Titus-CallerHostAddress";
    private static final String TITUS_HEADER_CALLER_ID = "X-Titus-CallerId";

    private final HostCallerIdResolver hostCallerIdResolver;

    @Inject
    public TitusHeaderHttpCallerIdResolver(HostCallerIdResolver hostCallerIdResolver) {
        this.hostCallerIdResolver = hostCallerIdResolver;
    }

    @Override
    public Optional<String> resolve(HttpServletRequest httpServletRequest) {
        String callerHost = Optional.ofNullable(httpServletRequest.getHeader(TITUS_HEADER_CALLER_HOST_ADDRESS)).orElse(httpServletRequest.getRemoteHost());
        String hostWithApp = hostCallerIdResolver.resolve(callerHost).map(app -> app + '(' + callerHost + ')').orElse(callerHost);
        String callerId = httpServletRequest.getHeader(TITUS_HEADER_CALLER_ID);
        return Optional.of(callerId == null ? hostWithApp : callerId + ';' + hostWithApp);
    }
}
