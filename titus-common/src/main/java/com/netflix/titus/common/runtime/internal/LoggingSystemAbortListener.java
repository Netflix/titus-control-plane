package com.netflix.titus.common.runtime.internal;

import com.netflix.titus.common.runtime.SystemAbortEvent;
import com.netflix.titus.common.runtime.SystemAbortListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingSystemAbortListener implements SystemAbortListener {

    private static final Logger logger = LoggerFactory.getLogger(LoggingSystemAbortListener.class);

    private static final LoggingSystemAbortListener INSTANCE = new LoggingSystemAbortListener();

    @Override
    public void onSystemAbortEvent(SystemAbortEvent event) {
        logger.error("JVM abort requested: {}", event);
    }

    public static LoggingSystemAbortListener getDefault() {
        return INSTANCE;
    }
}
