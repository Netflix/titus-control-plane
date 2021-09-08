package com.netflix.titus.api.jobmanager.model.job;

public class ContainerState {
    private final String containerName;
    private final ContainerHealth containerHealth;

    public ContainerState(String containerName, ContainerHealth containerHealth) {
        this.containerName = containerName;
        this.containerHealth = containerHealth;
    }

    public String getContainerName() {
        return containerName;
    }

    public ContainerHealth getContainerHealth() {
        return containerHealth;
    }

    public ContainerState.Builder toBuilder() {
        return newBuilder(this);
    }

    public static ContainerState.Builder newBuilder() { return new ContainerState.Builder();}

    public static ContainerState.Builder newBuilder(ContainerState containerState) {
        return new ContainerState.Builder()
                .withContainerHealth(containerState.getContainerHealth())
                .withContainerName(containerState.getContainerName());
    }

    public static final class Builder {
        private ContainerHealth containerHealth;
        private String containerName;

        private Builder() {
        }

        public ContainerState.Builder withContainerHealth(ContainerHealth containerHealth) {
            this.containerHealth = containerHealth;
            return this;
        }

        public ContainerState.Builder withContainerName(String containerName) {
            this.containerName = containerName;
            return this;
        }

        public ContainerState build() {
            return new ContainerState(containerName, containerHealth);
        }
    }

}