package pardo_test;

import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.List;

// TODO: Seems to be the case with TestStream, that session closure is not working properly?
public class SessionFn extends DoFn<KV<String, TestEvent>, List<TestEvent>> {

  private static final Duration SESSION_TIMEOUT = Duration.standardMinutes(15);

  @StateId("events")
  private final StateSpec<BagState<TestEvent>> eventsState = StateSpecs.bag();

  @StateId("lastTimerSet")
  private final StateSpec<ValueState<String>> variantIdState = StateSpecs.value();

  @StateId("lastSetTimerTime")
  private final StateSpec<ValueState<Instant>> lastSetTimerTimeState = StateSpecs.value();

  @TimerId("sessionClosed")
  private final TimerSpec sessionClosedSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

  @ProcessElement
  public void process(
      ProcessContext context,
      @StateId("events") BagState<TestEvent> events,
      @StateId("lastSetTimerTime") ValueState<Instant> lastSetTimerTime,
      @TimerId("sessionClosed") Timer expiryTimer) {
    System.out.println("Receiving event at: " + context.timestamp()); // debugging
    TestEvent event = context.element().getValue();
    if (lastSetTimerTime.read() != null) { // debugging
      if (lastSetTimerTime.read().isBefore(context.timestamp())) { // debugging
        System.out.println("Why didnt the timer fire?"); // debugging
      } // debugging
    } // debugging

    // Everytime we see an event in this session, reset the session-close timer
    Instant newTimerTime = event.getTimestamp().plus(SESSION_TIMEOUT);
    System.out.println("Resetting timer to : " + newTimerTime); // debugging
    expiryTimer.set(newTimerTime);
    lastSetTimerTime.write(newTimerTime); // debuggin

    //Also, add the event tot tne quq
    events.add(event);
  }

  @OnTimer("sessionClosed")
  public void onSessionClosed(
      OnTimerContext context,
      @StateId("events") BagState<TestEvent> events,
      @StateId("lastSetTimerTime") ValueState<Instant> lastSetTimerTime) {
    System.out.println("Timer firing at: " + context.timestamp());
    lastSetTimerTime.write(new Instant(Long.MAX_VALUE));
    context.output(ImmutableList.copyOf(events.read().iterator()));

    // Clear all of our state when done
    events.clear();

  }
}
