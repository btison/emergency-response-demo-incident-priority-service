package com.redhat.emergency.response.incident.priority.rules.model;

import java.math.BigDecimal;

public class IncidentAssignmentEvent {

    private String incident;

    private Boolean assigned;

    private BigDecimal lat;

    private BigDecimal lon;

    public IncidentAssignmentEvent(String incident, Boolean assigned, BigDecimal lat, BigDecimal lon) {
        this.incident = incident;
        this.assigned = assigned;
        this.lat = lat;
        this.lon = lon;
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

    public BigDecimal getLat() {
        return lat;
    }

    public void setLat(BigDecimal lat) {
        this.lat = lat;
    }

    public BigDecimal getLon() {
        return lon;
    }

    public void setLon(BigDecimal lon) {
        this.lon = lon;
    }
}
