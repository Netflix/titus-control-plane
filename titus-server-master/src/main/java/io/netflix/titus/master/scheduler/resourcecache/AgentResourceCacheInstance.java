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

package io.netflix.titus.master.scheduler.resourcecache;

import java.util.List;

public class AgentResourceCacheInstance {
    private final String id;
    private final List<AgentResourceCacheImage> images;
    private final List<AgentResourceCacheEni> enis;

    public AgentResourceCacheInstance(String id, List<AgentResourceCacheImage> images, List<AgentResourceCacheEni> enis) {
        this.id = id;
        this.images = images;
        this.enis = enis;
    }

    public String getId() {
        return id;
    }

    public List<AgentResourceCacheImage> getImages() {
        return images;
    }

    public List<AgentResourceCacheEni> getEnis() {
        return enis;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AgentResourceCacheInstance that = (AgentResourceCacheInstance) o;

        if (id != null ? !id.equals(that.id) : that.id != null) {
            return false;
        }
        if (images != null ? !images.equals(that.images) : that.images != null) {
            return false;
        }
        return enis != null ? enis.equals(that.enis) : that.enis == null;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (images != null ? images.hashCode() : 0);
        result = 31 * result + (enis != null ? enis.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "AgentResourceCacheInstance{" +
                "id='" + id + '\'' +
                ", images=" + images +
                ", enis=" + enis +
                '}';
    }
}
