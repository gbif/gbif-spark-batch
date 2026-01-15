package org.gbif.clustering.parsers;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class HaversineTest {

  @Test
  public void distanceTest() {
    double distance = Haversine.distance(21.8656, -102.909, 21.86558d, -102.90929d);
    assertTrue(distance < 0.2, "Distance exceeds 200m");
  }

  @Test
  public void distance1Test() {
    double distance = Haversine.distance(21.506, -103.092, 21.50599d, -103.09193);
    assertTrue(distance < 0.2, "Distance exceeds 200m");
  }
}
