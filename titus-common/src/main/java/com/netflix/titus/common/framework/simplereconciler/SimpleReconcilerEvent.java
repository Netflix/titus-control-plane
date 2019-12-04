/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.common.framework.simplereconciler;

import java.util.Objects;

public class SimpleReconcilerEvent<DATA> {

    public enum Kind {
        Added,
        Updated,
        Removed
    }

    private final Kind kind;
    private final String id;
    private final DATA data;

    /**
     * A unique identifier associated with each reconciler action and data element version. Consecutive transaction ids
     * are strictly increasing numbers, but they do not have to be continuous. Each managed data has its own transaction
     * id sequence. A client may use the transaction id, to chose the latest data version.
     */
    private final long transactionId;

    public SimpleReconcilerEvent(Kind kind, String id, DATA data, long transactionId) {
        this.kind = kind;
        this.id = id;
        this.data = data;
        this.transactionId = transactionId;
    }

    public Kind getKind() {
        return kind;
    }

    public String getId() {
        return id;
    }

    public DATA getData() {
        return data;
    }

    public long getTransactionId() {
        return transactionId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SimpleReconcilerEvent<?> that = (SimpleReconcilerEvent<?>) o;
        return transactionId == that.transactionId &&
                kind == that.kind &&
                Objects.equals(id, that.id) &&
                Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, id, data, transactionId);
    }

    @Override
    public String toString() {
        return "SimpleReconcilerEvent{" +
                "kind=" + kind +
                ", id='" + id + '\'' +
                ", data=" + data +
                ", transactionId='" + transactionId + '\'' +
                '}';
    }
}
