package com.netflix.titus.api.jobmanager.model.job.volume;

import javax.validation.Valid;

public class SharedContainerVolumeSource extends VolumeSource {

    @Valid
    private final String sourceContainer;

    @Valid
    private final String sourcePath;

    public SharedContainerVolumeSource(
            String sourceContainer,
            String sourcePath
    ) {
        this.sourceContainer = sourceContainer;
        this.sourcePath = sourcePath;
    }

    public String getSourceContainer() {
        return sourceContainer;
    }

    public String getSourcePath() {
        return sourcePath;
    }

    @Override
    public String toString() {
        return "VolumeSource{" +
                "sourceContainer='" + sourceContainer + '\'' +
                ", sourcePath='" + sourcePath + '\'' +
                '}';
    }

    @Override
    public int hashCode() {
        return sourceContainer.hashCode() + sourcePath.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SharedContainerVolumeSource that = (SharedContainerVolumeSource) o;
        if (!this.getSourceContainer().equals(that.getSourceContainer())) {
            return false;
        }
        return this.getSourcePath().equals(that.getSourcePath());
    }

    public static Builder newBuilder() {
        return new SharedContainerVolumeSource.Builder();
    }

    public static Builder newBuilder(SharedContainerVolumeSource sharedContainerVolume) {
        return new SharedContainerVolumeSource.Builder()
                .withSourceContainer(sharedContainerVolume.sourceContainer)
                .withSourcePath(sharedContainerVolume.sourcePath);
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
