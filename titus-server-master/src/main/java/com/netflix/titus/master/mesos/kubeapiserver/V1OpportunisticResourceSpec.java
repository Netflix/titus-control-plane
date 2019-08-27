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

package com.netflix.titus.master.mesos.kubeapiserver;

import java.util.Objects;

import com.google.gson.annotations.SerializedName;
import org.joda.time.DateTime;

/**
 * GSON-compatible POJO, with a default constructor following JavaBeans conventions.
 */
// TODO(fabio): autogenerate from the CRD swagger definition
public class V1OpportunisticResourceSpec {
    @SerializedName("capacity")
    private Capacity capacity;

    @SerializedName("window")
    private Window window;

    public Capacity getCapacity() {
        return capacity;
    }

    public void setCapacity(Capacity capacity) {
        this.capacity = capacity;
    }

    public Window getWindow() {
        return window;
    }

    public void setWindow(Window window) {
        this.window = window;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        V1OpportunisticResourceSpec that = (V1OpportunisticResourceSpec) o;
        return Objects.equals(capacity, that.capacity) &&
                Objects.equals(window, that.window);
    }

    @Override
    public int hashCode() {
        return Objects.hash(capacity, window);
    }

    @Override
    public String toString() {
        return "V1OpportunisticResourceSpec{" +
                "capacity=" + capacity +
                ", window=" + window +
                '}';
    }

    /**
     * GSON-compatible POJO, with a default constructor following JavaBeans conventions.
     */
    public static class Capacity {
        @SerializedName("cpu")
        private int cpu;

        public int getCpu() {
            return cpu;
        }

        public void setCpu(int cpu) {
            this.cpu = cpu;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Capacity capacity = (Capacity) o;
            return cpu == capacity.cpu;
        }

        @Override
        public int hashCode() {
            return Objects.hash(cpu);
        }

        @Override
        public String toString() {
            return "Capacity{" +
                    "cpu=" + cpu +
                    '}';
        }
    }

    /**
     * GSON-compatible POJO, with a default constructor following JavaBeans conventions.
     * <p>
     * This class uses Joda-Time types to make integrating with the kubernetes API client easier, since Joda-Time is
     * what is supported by the its built-in deserializers.
     *
     * @see io.kubernetes.client.JSON.DateTimeTypeAdapter
     */
    public static class Window {
        @SerializedName("start")
        private DateTime start;

        @SerializedName("end")
        private DateTime end;

        public DateTime getStart() {
            return start;
        }

        public void setStart(DateTime start) {
            this.start = start;
        }

        public DateTime getEnd() {
            return end;
        }

        public void setEnd(DateTime end) {
            this.end = end;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Window window = (Window) o;
            return Objects.equals(start, window.start) &&
                    Objects.equals(end, window.end);
        }

        @Override
        public int hashCode() {
            return Objects.hash(start, end);
        }

        @Override
        public String toString() {
            return "Window{" +
                    "start=" + start +
                    ", end=" + end +
                    '}';
        }
    }
}
