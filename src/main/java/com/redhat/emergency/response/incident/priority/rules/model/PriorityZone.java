package com.redhat.emergency.response.incident.priority.rules.model;

import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;

public class PriorityZone {

    private String id;

    private double lat;

    private double lon;

    private double radius;

    public PriorityZone(String id, double lat, double lon, double radius) {
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

    public double getLat() {
        return this.lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public double getLon() {
        return this.lon;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }

    public double getRadius() {
        return this.radius;
    }

    public void setRadius(double radius) {
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
