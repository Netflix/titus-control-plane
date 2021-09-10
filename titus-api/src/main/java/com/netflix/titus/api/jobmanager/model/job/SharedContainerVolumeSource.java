package com.netflix.titus.api.jobmanager.model.job;

import javax.validation.Valid;

public class SharedContainerVolumeSource {


    @Valid
    private final String sourceContainer;

    @Valid
    private final String sourcePath;

    public  SharedContainerVolumeSource(
            String sourceContainer,
            String sourcePath
    ) {
        this.sourceContainer = sourceContainer;
        this.sourcePath = sourcePath;
    }

    public static SharedContainerVolumeSource.Builder newBuilder() {
        return new SharedContainerVolumeSource.Builder();
    }

    public static SharedContainerVolumeSource.Builder newBuilder(SharedContainerVolumeSource sharedContainerVolumeSource) {
        return new SharedContainerVolumeSource.Builder()
                .withSourceContainer(sharedContainerVolumeSource.sourceContainer)
                .withSourcePath(sharedContainerVolumeSource.sourcePath);
    }

    public String getSourceContainer() {
        return sourceContainer;
    }

    public String getSourcePath() {
        return sourcePath;
    }

    public static final class Builder {
        String sourceContainer;
        String sourcePath;

        public SharedContainerVolumeSource.Builder withSourceContainer(String sourceContainer) {
            this.sourceContainer = sourceContainer;
            return this;
        }

        public SharedContainerVolumeSource.Builder withSourcePath(String sourcePath) {
            this.sourcePath = sourcePath;
            return this;
        }

        public SharedContainerVolumeSource build() {
            return new SharedContainerVolumeSource(
                    sourceContainer,
                    sourcePath
            );
        }
    }

}
