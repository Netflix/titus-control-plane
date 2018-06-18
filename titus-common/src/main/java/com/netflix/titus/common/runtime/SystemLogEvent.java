package com.netflix.titus.common.runtime;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * System logger event (see {@link SystemLogService}).
 */
public class SystemLogEvent {

    /**
     * The criticality level of the log event.
     */
    public enum Priority {
        /**
         * Use when system operates normally, to log an information about an important system change. For example
         * becoming a new leader, an important configuration change, etc.
         */
        Info,

        /**
         * Use when erroneous but non-critical things happens. For example, invariant violations that can be fixed, but
         * indicate potential issues either in the code or the user provided data.
         */
        Warn,

        /**
         * Use when the system ability to make progress with its work is impaired or a major invariant violations are
         * discovered. For example, prolonged lack of connectivity with AWS service, corrupted data detected in a
         * core system component, etc.
         */
        Error,

        /**
         * Use for major issues, which may result in an incorrect system behavior or forced system shutdown.
         */
        Fatal
    }

    /**
     * High level classifier for the event, that can be used as a key aggregator by an event processing system.
     * For example, by classifying all external connectivity errors as {@link Category#ConnectivityError}, it easy
     * to filter them out and detect common connectivity issues.
     */
    public enum Category {
        /**
         * A transient error.
         */
        Transient,

        /**
         * Permanent error. System is not able to recover from it without help from the administrator.
         */
        Permanent,

        /**
         * Connectivity issues with an external system (database, AWS, etc).
         */
        ConnectivityError,

        /**
         * System invariant violations (for example corrupted data discovered in the core processing logic).
         */
        InvariantViolation,

        /**
         * Use when none of the existing categories is applicable.
         */
        Other
    }

    /**
     * Event category (see {@link Category}).
     */
    private final Category category;

    /**
     * Event category (see {@link Priority}).
     */
    private final Priority priority;

    /**
     * Name of a component that creates the event.
     */
    private final String component;

    /**
     * A short description of an event.
     */
    private final String message;

    /**
     * A collection of tags for further event classification.
     */
    private final Set<String> tags;

    /**
     * Additional information giving more insight into what happened. For example, if corrupted task data are found,
     * the context may include the job and task ids.
     */
    private final Map<String, String> context;

    private SystemLogEvent(Category category,
                           Priority priority,
                           String component,
                           String message,
                           Set<String> tags,
                           Map<String, String> context) {
        this.category = category;
        this.priority = priority;
        this.component = component;
        this.message = message;
        this.tags = tags;
        this.context = context;
    }

    public Category getCategory() {
        return category;
    }

    public Priority getPriority() {
        return priority;
    }

    public String getComponent() {
        return component;
    }

    public String getMessage() {
        return message;
    }

    public Set<String> getTags() {
        return tags;
    }

    public Map<String, String> getContext() {
        return context;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SystemLogEvent that = (SystemLogEvent) o;
        return category == that.category &&
                priority == that.priority &&
                Objects.equals(component, that.component) &&
                Objects.equals(message, that.message) &&
                Objects.equals(tags, that.tags) &&
                Objects.equals(context, that.context);
    }

    @Override
    public int hashCode() {

        return Objects.hash(category, priority, component, message, tags, context);
    }

    @Override
    public String toString() {
        return "SystemLogEvent{" +
                "category=" + category +
                ", priority=" + priority +
                ", component='" + component + '\'' +
                ", message='" + message + '\'' +
                ", tags=" + tags +
                ", context=" + context +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private Category category;
        private Priority priority;
        private String component;
        private String message;
        private Set<String> tags;
        private Map<String, String> context;

        private Builder() {
        }

        public Builder withCategory(Category category) {
            this.category = category;
            return this;
        }

        public Builder withPriority(Priority priority) {
            this.priority = priority;
            return this;
        }

        public Builder withComponent(String component) {
            this.component = component;
            return this;
        }

        public Builder withMessage(String message) {
            this.message = message;
            return this;
        }

        public Builder withTags(Set<String> tags) {
            this.tags = tags;
            return this;
        }

        public Builder withContext(Map<String, String> context) {
            this.context = context;
            return this;
        }

        public Builder but() {
            return newBuilder().withCategory(category).withPriority(priority).withComponent(component).withMessage(message).withContext(context);
        }

        public SystemLogEvent build() {
            return new SystemLogEvent(category, priority, component, message, tags, context);
        }
    }
}
