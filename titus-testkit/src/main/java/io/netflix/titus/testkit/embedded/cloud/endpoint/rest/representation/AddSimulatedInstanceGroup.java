package io.netflix.titus.testkit.embedded.cloud.endpoint.rest.representation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class AddSimulatedInstanceGroup {

    private final String id;
    private final String instanceType;
    private final int min;
    private final int desired;
    private final int max;

    @JsonCreator
    public AddSimulatedInstanceGroup(@JsonProperty("id") String id,
                                     @JsonProperty("instanceType") String instanceType,
                                     @JsonProperty("min") int min,
                                     @JsonProperty("desired") int desired,
                                     @JsonProperty("max") int max) {
        this.id = id;
        this.instanceType = instanceType;
        this.min = min;
        this.desired = desired;
        this.max = max;
    }

    public String getId() {
        return id;
    }

    public String getInstanceType() {
        return instanceType;
    }

    public int getMin() {
        return min;
    }

    public int getDesired() {
        return desired;
    }

    public int getMax() {
        return max;
    }
}
