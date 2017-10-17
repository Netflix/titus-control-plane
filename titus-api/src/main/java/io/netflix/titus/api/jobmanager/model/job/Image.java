/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.api.jobmanager.model.job;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import io.netflix.titus.common.model.sanitizer.ClassInvariant;

/**
 * Image reference.
 */
@ClassInvariant(condition = "tag != null || digest != null", message = "Both tag and digest missing")
public class Image {

    @NotNull
    @Pattern(regexp = "[a-zA-Z0-9\\.\\\\/_-]+", message = "'#{#root}' is not valid docker image name")
    private final String name;

    @Pattern(regexp = "[a-zA-Z0-9\\._-]+", message = "'#{#root}' is not valid docker image tag")
    private final String tag;

    private final String digest;

    public Image(String name, String tag, String digest) {
        this.name = name;
        this.tag = tag;
        this.digest = digest;
    }

    public String getName() {
        return name;
    }

    public String getTag() {
        return tag;
    }

    public String getDigest() {
        return digest;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Image image = (Image) o;

        if (name != null ? !name.equals(image.name) : image.name != null) {
            return false;
        }
        if (tag != null ? !tag.equals(image.tag) : image.tag != null) {
            return false;
        }
        return digest != null ? digest.equals(image.digest) : image.digest == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (tag != null ? tag.hashCode() : 0);
        result = 31 * result + (digest != null ? digest.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Image{" +
                "name='" + name + '\'' +
                ", tag='" + tag + '\'' +
                ", digest='" + digest + '\'' +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(Image image) {
        return new Builder(image);
    }

    public static final class Builder {
        private String name;
        private String tag;
        private String digest;

        private Builder() {
        }

        private Builder(Image image) {
            this.name = image.getName();
            this.tag = image.getTag();
            this.digest = image.getDigest();
        }

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withTag(String tag) {
            this.tag = tag;
            return this;
        }

        public Builder withDigest(String digest) {
            this.digest = digest;
            return this;
        }

        public Image build() {
            Image image = new Image(name, tag, digest);
            return image;
        }
    }
}
