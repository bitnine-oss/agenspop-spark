package net.bitnine.agenspopspark.dto;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter @Setter @ToString
public class JobMessage {
    private final String jobId;      // jobName : job-<sequence>
    private final int totalSteps;
    private int currentStep;
    private String elapsedTime;
    private String state;
    private String progress;        // last string of history

    public JobMessage(String jobId, int totalSteps) {
        this.totalSteps = totalSteps;
        this.jobId = jobId;
    }
}
