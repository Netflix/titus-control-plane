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

package com.netflix.titus.api.jobmanager.model.job.sanitizer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;
import java.util.function.Function;

import com.netflix.titus.api.model.EfsMount;

import static com.netflix.titus.common.util.CollectionsExt.first;
import static java.lang.String.format;

/**
 * {@link EfsMount} cleanup includes:
 * <ul>
 * <li>Adding default mount mode</li>
 * <li>Sorting mounts according to EFS mount point (shortest first)</li>
 * </ul>
 */
public class EfsMountsSanitizer implements Function<Object, Optional<Object>> {
    @Override
    public Optional<Object> apply(Object oneEfsMountOrCollection) {
        if (oneEfsMountOrCollection instanceof EfsMount) {
            return applyEfsMount((EfsMount) oneEfsMountOrCollection);
        }
        if (oneEfsMountOrCollection instanceof Collection) {
            Collection<EfsMount> collection = (Collection<EfsMount>) oneEfsMountOrCollection;
            if (collection.isEmpty()) {
                return Optional.empty();
            }
            if (first(collection) instanceof EfsMount) {
                return applyEfsMounts(collection);
            }
        }
        throw new IllegalArgumentException(
                format("%s can be applied on %s or its collection", EfsMountsSanitizer.class, EfsMount.class)
        );
    }

    private Optional<Object> applyEfsMount(EfsMount efsMount) {
        if (efsMount.getMountPerm() == null) {
            return Optional.of(EfsMount.newBuilder(efsMount).withMountPerm(EfsMount.MountPerm.RW).build());
        }
        return Optional.empty();
    }

    private Optional<Object> applyEfsMounts(Collection<EfsMount> efsMounts) {
        ArrayList<EfsMount> sorted = new ArrayList<>(efsMounts);
        sorted.sort(Comparator.comparing(EfsMount::getMountPoint));
        for (int i = 0; i < sorted.size(); i++) {
            int current = i;
            applyEfsMount(sorted.get(i)).ifPresent(fixed -> sorted.set(current, (EfsMount) fixed));
        }
        return Optional.of(sorted);
    }
}
