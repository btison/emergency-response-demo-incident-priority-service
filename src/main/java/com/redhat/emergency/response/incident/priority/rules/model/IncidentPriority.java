package com.redhat.emergency.response.incident.priority.rules.model;

public class IncidentPriority {

    private String incident;

    private Integer priority;

    public IncidentPriority(String incident) {
        this.incident = incident;
        this.priority = 0;
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
}
