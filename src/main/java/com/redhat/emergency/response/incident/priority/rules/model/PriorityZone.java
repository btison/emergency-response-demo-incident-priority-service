package com.redhat.emergency.response.incident.priority.rules.model;

import java.math.BigDecimal;

public class PriorityZone {

    private String id;

    private BigDecimal lat;

    private BigDecimal lon;

    private BigDecimal radius;

    public PriorityZone(String id, BigDecimal lat, BigDecimal lon, BigDecimal radius) {
        this.id = id;
        this.lat = lat;
        this.lon = lon;
        this.radius = radius;
    }

    public String getId() {
        return this.id;
    }

    public void setId(String id) {
        this.id = id;
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

    public BigDecimal getRadius() {
        return this.radius;
    }

    public void setRadius(BigDecimal radius) {
        this.radius = radius;
    }

    @Override
    public String toString() {
        return "{" +
            " id='" + getId() + "'" +
            ", lat='" + getLat() + "'" +
            ", lon='" + getLon() + "'" +
            ", radius='" + getRadius() + "'" +
            "}";
    }
}
