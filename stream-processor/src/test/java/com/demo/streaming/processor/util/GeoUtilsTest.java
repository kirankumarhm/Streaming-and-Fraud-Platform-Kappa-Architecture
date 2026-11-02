package com.demo.streaming.processor.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class GeoUtilsTest {

    @Test
    @DisplayName("Calculate great-circle distance between NYC and London accurately (~5570 km)")
    void calculateHaversineDistanceBetweenNycAndLondon() {
        double nycLat = 40.7128;
        double nycLon = -74.0060;

        double londonLat = 51.5074;
        double londonLon = -0.1278;

        double distanceKm = GeoUtils.calculateHaversineDistanceKm(nycLat, nycLon, londonLat, londonLon);

        // Actual geodesic distance is ~5570 km
        assertThat(distanceKm).isCloseTo(5570.0, within(50.0));
    }

    @Test
    @DisplayName("Zero distance for identical coordinates")
    void identicalCoordinatesZeroDistance() {
        double distance = GeoUtils.calculateHaversineDistanceKm(37.7749, -122.4194, 37.7749, -122.4194);
        assertThat(distance).isEqualTo(0.0);
    }
}
