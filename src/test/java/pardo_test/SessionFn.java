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

  @StateId("lastSetTimerTime")
  private final StateSpec<ValueState<Instant>> variantIdState = StateSpecs.value();

  @StateId("key")
  private final StateSpec<ValueState<String>> keyState = StateSpecs.value();

  @TimerId("sessionClosed")
  private final TimerSpec sessionClosedSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

  @ProcessElement
  public void process(
      ProcessContext context,
      @StateId("events") BagState<TestEvent> events,
      @StateId("lastSetTimerTime") ValueState<Instant> lastSetTimerTime,
      @TimerId("sessionClosed") Timer expiryTimer,
      @StateId("key") ValueState<String> key) {
    TestEvent event = context.element().getValue();

    System.out.println("[" + context.element().getKey() + "] Receiving event at: " + context.timestamp()); // debugging
    if (lastSetTimerTime.read() != null) { // debugging
      if (lastSetTimerTime.read().isBefore(context.timestamp())) { // debugging
        System.out.println("[" + context.element().getKey() + "] Why didnt the timer fire?"); // debugging

        // // FIX: If we manually check for expired timer and perform our flush operation here before proceeding,
        // // everything works exactly as expected! Shouldn't the Beam runtime do this check for me before calling
        // // process()?
        // lastSetTimerTime.write(new Instant(Long.MAX_VALUE));
        // context.output(ImmutableList.copyOf(events.read().iterator()));
        // events.clear();
      } // debugging
    } // debugging

    // Everytime we see an event in this session, reset the session-close timer
    Instant newTimerTime = event.getTimestamp().plus(SESSION_TIMEOUT);
    System.out.println("[" + context.element().getKey() + "] Resetting timer to : " + newTimerTime); // debugging
    expiryTimer.set(newTimerTime);
    lastSetTimerTime.write(newTimerTime); // debugging

    // Store the key since its not easily accessible in the timer expiry context, so we log with it
    key.write(context.element().getKey());

    // Add the event to the queue
    events.add(event);
  }

  @OnTimer("sessionClosed")
  public void onSessionClosed(
      OnTimerContext context,
      @StateId("events") BagState<TestEvent> events,
      @StateId("lastSetTimerTime") ValueState<Instant> lastSetTimerTime,
      @StateId("key") ValueState<String> key) {
    System.out.println("[" + key.read() + "] Timer firing at: " + context.timestamp());
    lastSetTimerTime.write(new Instant(Long.MAX_VALUE));
    context.output(ImmutableList.copyOf(events.read().iterator()));

    // Clear all of our state when done
    events.clear();
  }
}
