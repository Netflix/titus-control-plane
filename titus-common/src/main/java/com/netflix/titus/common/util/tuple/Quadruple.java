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

package com.netflix.titus.common.util.tuple;

/**
 */
public class Quadruple<A, B, C, D> {

    private final A first;
    private final B second;
    private final C thrid;
    private final D fourth;

    public Quadruple(A first, B second, C thrid, D fourth) {
        this.first = first;
        this.second = second;
        this.thrid = thrid;
        this.fourth = fourth;
    }

    public A getFirst() {
        return first;
    }

    public B getSecond() {
        return second;
    }

    public C getThrid() {
        return thrid;
    }

    public D getFourth() {
        return fourth;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Quadruple<?, ?, ?, ?> quadruple = (Quadruple<?, ?, ?, ?>) o;

        if (first != null ? !first.equals(quadruple.first) : quadruple.first != null) {
            return false;
        }
        if (second != null ? !second.equals(quadruple.second) : quadruple.second != null) {
            return false;
        }
        if (thrid != null ? !thrid.equals(quadruple.thrid) : quadruple.thrid != null) {
            return false;
        }
        return fourth != null ? fourth.equals(quadruple.fourth) : quadruple.fourth == null;
    }

    @Override
    public int hashCode() {
        int result = first != null ? first.hashCode() : 0;
        result = 31 * result + (second != null ? second.hashCode() : 0);
        result = 31 * result + (thrid != null ? thrid.hashCode() : 0);
        result = 31 * result + (fourth != null ? fourth.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Quadruple{" +
                "first=" + first +
                ", second=" + second +
                ", thrid=" + thrid +
                ", fourth=" + fourth +
                '}';
    }

    public static <A, B, C, D> Quadruple<A, B, C, D> of(A first, B second, C third, D fourth) {
        return new Quadruple<>(first, second, third, fourth);
    }
}
