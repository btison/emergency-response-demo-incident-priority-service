package com.redhat.emergency.response.incident.priority.rules.model;

public class AveragePriority {

    private Double averagePriority;

    private boolean needsEvaluation;

    public AveragePriority() {
        averagePriority = 0.0;
        needsEvaluation = false;
    }

    public Double getAveragePriority() {
        return averagePriority;
    }

    public void setAveragePriority(Double averagePriority) {
        this.averagePriority = averagePriority;
    }

    public boolean isNeedsEvaluation() {
        return needsEvaluation;
    }

    public void setNeedsEvaluation(boolean needsEvaluation) {
        this.needsEvaluation = needsEvaluation;
    }
}
