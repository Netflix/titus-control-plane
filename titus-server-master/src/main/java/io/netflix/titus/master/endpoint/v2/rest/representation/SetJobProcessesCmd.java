package io.netflix.titus.master.endpoint.v2.rest.representation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SetJobProcessesCmd {
    private static Logger logger = LoggerFactory.getLogger(SetJobProcessesCmd.class);
    private String user;
    private String jobId;
    private boolean disableIncreaseDesired;
    private boolean disableDecreaseDesired;

    @JsonCreator
    public SetJobProcessesCmd(@JsonProperty("user") String user,
                              @JsonProperty("jobId") String jobId,
                              @JsonProperty("disableIncreaseDesired") boolean disableIncreaseDesired,
                              @JsonProperty("disableDecreaseDesired") boolean disableDecreaseDesired) {
        this.user = user;
        this.jobId = jobId;
        this.disableIncreaseDesired = disableIncreaseDesired;
        this.disableDecreaseDesired = disableDecreaseDesired;
    }

    public String getUser() {
        return user;
    }

    public String getJobId() {
        return jobId;
    }

    public boolean isDisableIncreaseDesired() {
        return disableIncreaseDesired;
    }

    public boolean isDisableDecreaseDesired() {
        return disableDecreaseDesired;
    }

    @Override
    public String toString() {
        return "SetJobProcessesCmd{" +
                "user='" + user + '\'' +
                ", jobId='" + jobId + '\'' +
                ", disableIncreaseDesired=" + disableIncreaseDesired +
                ", disableDecreaseDesired=" + disableDecreaseDesired +
                '}';
    }
}
