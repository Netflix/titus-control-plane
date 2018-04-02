/*
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.titus.master.scheduler.resourcecache;

public class AgentResourceCacheImage {
    private final String imageName;
    private final String digest;
    private final String tag;

    public AgentResourceCacheImage(String imageName, String digest, String tag) {
        this.imageName = imageName;
        this.digest = digest;
        this.tag = tag;
    }

    public String getImageName() {
        return imageName;
    }

    public String getDigest() {
        return digest;
    }

    public String getTag() {
        return tag;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AgentResourceCacheImage that = (AgentResourceCacheImage) o;

        if (imageName != null ? !imageName.equals(that.imageName) : that.imageName != null) {
            return false;
        }
        if (digest != null ? !digest.equals(that.digest) : that.digest != null) {
            return false;
        }
        return tag != null ? tag.equals(that.tag) : that.tag == null;
    }

    @Override
    public int hashCode() {
        int result = imageName != null ? imageName.hashCode() : 0;
        result = 31 * result + (digest != null ? digest.hashCode() : 0);
        result = 31 * result + (tag != null ? tag.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "AgentResourceCacheImage{" +
                "imageName='" + imageName + '\'' +
                ", digest='" + digest + '\'' +
                ", tag='" + tag + '\'' +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder(this);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(AgentResourceCacheImage image) {
        return new Builder().withImageName(image.getImageName())
                .withDigest(image.getDigest())
                .withTag(image.getTag());
    }

    public static final class Builder {
        private String imageName;
        private String digest;
        private String tag;

        private Builder() {
        }

        public Builder withImageName(String imageName) {
            this.imageName = imageName;
            return this;
        }

        public Builder withDigest(String digest) {
            this.digest = digest;
            return this;
        }

        public Builder withTag(String tag) {
            this.tag = tag;
            return this;
        }

        public Builder but() {
            return newBuilder().withImageName(imageName).withDigest(digest).withTag(tag);
        }

        public AgentResourceCacheImage build() {
            return new AgentResourceCacheImage(imageName, digest, tag);
        }
    }
}
