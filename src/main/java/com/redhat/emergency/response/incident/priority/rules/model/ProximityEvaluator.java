package com.redhat.emergency.response.incident.priority.rules.model;

import java.math.BigDecimal;

public class ProximityEvaluator {

    public ProximityEvaluator() {
    }

    public boolean inPriorityZone(IncidentPriority incident, PriorityZone priorityZone) {
        //return true if priority zone radius is greater than the distance between the pz center and incident location
        return priorityZone.getRadius().compareTo(
            new BigDecimal(distance(
                incident.getLat().doubleValue(), 
                incident.getLon().doubleValue(), 
                priorityZone.getLat().doubleValue(), 
                priorityZone.getLon().doubleValue(), "K")
            )
         ) >= 0;
    }

    /**
     * Calculate the distance between two coordinates in latitude and longitude, using the specified units.
     * 
     * @param lat1 the latitude of the first point
     * @param lon1 the longitude of the first point
     * @param lat2 the latitude of the second point
     * @param lon2 the longitude of the second point
     * @param unit the unit of measurement, where K = kilometers, M = miles (defualt), and N = nautical miles
     * @return the distance as a double
     */
    private double distance(double lat1, double lon1, double lat2, double lon2, String unit) {
		if ((lat1 == lat2) && (lon1 == lon2)) {
			return 0;
		}
		else {
			double theta = lon1 - lon2;
			double dist = Math.sin(Math.toRadians(lat1)) * Math.sin(Math.toRadians(lat2)) + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.cos(Math.toRadians(theta));
			dist = Math.acos(dist);
			dist = Math.toDegrees(dist);
			dist = dist * 60 * 1.1515;
			if (unit.equals("K")) {
				dist = dist * 1.609344;
			} else if (unit.equals("N")) {
				dist = dist * 0.8684;
			}
			return (dist);
		}
	}
}
