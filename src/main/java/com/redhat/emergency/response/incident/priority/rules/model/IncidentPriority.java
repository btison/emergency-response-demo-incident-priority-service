package com.redhat.emergency.response.incident.priority.rules.model;

import java.math.BigDecimal;

public class IncidentPriority {

    private String incident;

    private Integer priority;

    private boolean escalated;

    private BigDecimal lat;

    private BigDecimal lon;

    public IncidentPriority(String incident, BigDecimal lat, BigDecimal lon) {
        this.incident = incident;
        this.priority = 0;
        this.escalated = false;
        this.lat = lat;
        this.lon = lon;
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

    public boolean getEscalated() {
        return escalated;
    }

    public void setEscalated(boolean escalated) {
        this.escalated = escalated;
    }

    public BigDecimal getLat() {
        return this.lat;
    }

    public void setLat(BigDecimal lat) {
        this.lat = lat;
    }

    public BigDecimal getLon() {
        return this.lon;
    }

    public void setLon(BigDecimal lon) {
        this.lon = lon;
    }
}
