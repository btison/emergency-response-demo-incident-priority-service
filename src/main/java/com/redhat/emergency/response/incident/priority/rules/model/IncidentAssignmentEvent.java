package com.redhat.emergency.response.incident.priority.rules.model;

public class IncidentAssignmentEvent {

    private String incident;

    private Boolean assigned;

    public IncidentAssignmentEvent(String incident, Boolean assigned) {
        this.incident = incident;
        this.assigned = assigned;
    }

    public String getIncident() {
        return incident;
    }

    public void setIncident(String incident) {
        this.incident = incident;
    }

    public Boolean getAssigned() {
        return assigned;
    }

    public void setAssigned(Boolean assigned) {
        this.assigned = assigned;
    }
}
