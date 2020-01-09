package com.redhat.emergency.response.incident.priority.rules.model;

public class IncidentPriority {

    private String incident;

    private Integer priority;

    private boolean needsEscalation;

    private boolean escalated;

    public IncidentPriority(String incident) {
        this.incident = incident;
        this.priority = 0;
        this.needsEscalation = false;
    }

    public String getIncident() {
        return incident;
    }

    public void setIncident(String incident) {
        this.incident = incident;
    }

    public Integer getPriority() {
        return priority;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
    }


    public boolean getNeedsEscalation() {
        return needsEscalation;
    }

    public void setNeedsEscalation(boolean needsEscalation) {
        this.needsEscalation = needsEscalation;
    }

    public boolean getEscalated() {
        return escalated;
    }

    public void setEscalated(boolean escalated) {
        this.escalated = escalated;
    }
}
