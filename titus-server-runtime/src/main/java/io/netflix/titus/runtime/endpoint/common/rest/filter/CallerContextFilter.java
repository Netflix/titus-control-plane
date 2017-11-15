package io.netflix.titus.runtime.endpoint.common.rest.filter;

import java.io.IOException;
import java.util.Optional;
import javax.inject.Singleton;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

/**
 * Jersey does not expose all {@link javax.servlet.http.HttpServletRequest} data. To make it available we inject
 * them in this filter into thread local variable.
 */
@Singleton
public class CallerContextFilter implements Filter {

    private static final ThreadLocal<String> currentCallerAddress = new ThreadLocal<>();

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        currentCallerAddress.set(request.getRemoteAddr());
        try {
            chain.doFilter(request, response);
        } finally {
            currentCallerAddress.set(null);
        }
    }

    @Override
    public void destroy() {
    }

    public static Optional<String> getCurrentCallerAddress() {
        return Optional.ofNullable(currentCallerAddress.get());
    }
}
