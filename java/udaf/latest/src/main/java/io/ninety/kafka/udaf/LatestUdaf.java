package io.ninety.kafka.udaf;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;

@UdafDescription(
name = "latest",
description = "Return the latest value.")

public final class LatestUdaf {

  private LatestUdaf() {
    // just to make the checkstyle happy
  }

  private static <T> Udaf<T, T> latestCollector() {
    return new Udaf<T, T>() {

      @Override
      public T initialize() {
        return null;
      }

      @Override
      public T aggregate(final T thisValue, final T aggregate) {
        return thisValue;
      }

      @Override
      public T merge(final T aggOne, final T aggTwo) {
        return aggOne;
      }
    };
  }

  @UdafFactory(description = "get latest Bigint field")
  public static Udaf<Long, Long> createLatestLong() {
    return latestCollector();
  }

  @UdafFactory(description = "get latest Integer field")
  public static Udaf<Integer, Integer> createLatestInt() {
    return latestCollector();
  }

  @UdafFactory(description = "get latest Double field")
  public static Udaf<Double, Double> createLatestDouble() {
    return latestCollector();
  }

  @UdafFactory(description = "get latest String field")
  public static Udaf<String, String> createLatestString() {
    return latestCollector();
  }

  @UdafFactory(description = "get latest Bool field")
  public static Udaf<Boolean, Boolean> createLatestBool() {
    return latestCollector();
  }
}
