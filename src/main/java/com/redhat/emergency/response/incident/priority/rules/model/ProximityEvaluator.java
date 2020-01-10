package com.redhat.emergency.response.incident.priority.rules.model;

public class ProximityEvaluator {

    public ProximityEvaluator() {
    }

    public boolean inPriorityZone(String incidentId, PriorityZone priorityZone) {
        // WebClient client = WebClient.create(Vertx.vertx());
        // AtomicBoolean inPriorityZone = new AtomicBoolean(false);
        // client.get("http://user4-incident-service.apps.cluster-e222.e222.example.opentlc.com/incidents/incident/" + incidentId)
        //     .send(ar -> {
        //         if (ar.succeeded()) {
        //             JsonObject response = ar.result().bodyAsJsonObject();
              
        //             double incidentLat = Double.parseDouble(response.getString("lat"));
        //             double incidentLon = Double.parseDouble(response.getString("lon"));

        //             inPriorityZone.set(distance(incidentLat, incidentLon, priorityZone.getLat(), priorityZone.getLon(), "K") <= priorityZone.getRadius());
        //           } else {
        //             System.out.println("Something went wrong " + ar.cause().getMessage());
        //           }
        //     });

        // client.close();
        // return inPriorityZone.get();
        double incidentLat = Double.parseDouble("34.19439");
        double incidentLon = Double.parseDouble("-77.81453");

        return distance(incidentLat, incidentLon, priorityZone.getLat(), priorityZone.getLon(), "K") <= priorityZone.getRadius();
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
