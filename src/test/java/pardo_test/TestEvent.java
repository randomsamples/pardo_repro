package pardo_test;

import org.joda.time.Instant;

import java.io.Serializable;
import java.util.Objects;

public class TestEvent implements Serializable {
  private String eventType;
  private Instant timestamp;

  public TestEvent(String eventType, Instant timestamp) {
    this.eventType = eventType;
    this.timestamp = timestamp;
  }

  public String getEventType() {
    return this.eventType;
  }

  public Instant getTimestamp() {
    return this.timestamp;
  }

  public boolean equals(Object o) {
    // self check
    if (this == o)
      return true;
    // null check
    if (o == null)
      return false;
    // type check and cast
    if (getClass() != o.getClass())
      return false;
    TestEvent testEvent = (TestEvent) o;
    // field comparison
    return Objects.equals(eventType, testEvent.eventType)
        && Objects.equals(timestamp, testEvent.timestamp);
  }

  @Override
  public int hashCode() {
    return eventType.hashCode() ^ timestamp.hashCode();
  }
}