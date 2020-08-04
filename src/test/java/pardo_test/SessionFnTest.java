package pardo_test;

import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;

import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class SessionFnTest implements Serializable {

  private static final Instant starTime = Instant.parse("2000-01-01T00:00:00Z");
  private static final Duration period = Duration.standardMinutes(5);
  private static final Duration gap = Duration.standardMinutes(60);

  private static Instant makeTimestamp(int numGaps, int eventSequenceNumber) {
    return starTime.plus(gap.multipliedBy(numGaps).plus(period.multipliedBy(eventSequenceNumber)));
  }

  private static TestStream<KV<String,TestEvent>> createTestStream() {
    TestStream.Builder<KV<String,TestEvent>> testStream = TestStream.create(
        KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(TestEvent.class)));


    List<KV<String,TestEvent>> testEvents = Arrays.asList(
        KV.of("Key1", new TestEvent("Key 1 - Session 1 - Event 1", makeTimestamp(0 /* gaps */, 0 /* event seq num */))),

        KV.of("Key2", new TestEvent("Key 2 - Session 1 - Event 1", makeTimestamp(0, 1))),
        KV.of("Key2", new TestEvent("Key 2 - Session 1 - Event 2", makeTimestamp(0, 2))),

        null, // Add a watermark advancement larger than session gap, should split these sessions by firing the timers

        KV.of("Key1", new TestEvent("Key 1 - Session 2 - Event 1", makeTimestamp(1, 3))),
        KV.of("Key2", new TestEvent("Key 2 - Session 2 - Event 1", makeTimestamp(1, 4))),
        KV.of("Key1", new TestEvent("Key 1 - Session 2 - Event 2", makeTimestamp(1, 5))),
        KV.of("Key2", new TestEvent("Key 2 - Session 2 - Event 2", makeTimestamp(1, 6))),
        KV.of("Key2", new TestEvent("Key 2 - Session 2 - Event 3", makeTimestamp(1, 7))),

        null,

        KV.of("Key1", new TestEvent("Key 1 - Session 3 - Event 1", makeTimestamp(2, 8))),
        KV.of("Key1", new TestEvent("Key 1 - Session 3 - Event 2", makeTimestamp(2, 9))),
        KV.of("Key3", new TestEvent("Key 3 - Session 1 - Event 1", makeTimestamp(2, 10))),
        KV.of("Key2", new TestEvent("Key 2 - Session 3 - Event 1", makeTimestamp(2, 11))),
        KV.of("Key3", new TestEvent("Key 3 - Session 1 - Event 2", makeTimestamp(2, 12))),
        KV.of("Key2", new TestEvent("Key 2 - Session 3 - Event 2", makeTimestamp(2, 13)))
        );

    Instant previousWatermark = new Instant(0);
    for (KV<String,TestEvent> event : testEvents) {
      if (event == null) {
        testStream.advanceWatermarkTo(previousWatermark.plus(Duration.standardMinutes(16)));
      } else {
        Instant eventTimestamp = event.getValue().getTimestamp();
        testStream = testStream.addElements(TimestampedValue.of(event, eventTimestamp));
        testStream = testStream.advanceWatermarkTo(eventTimestamp);
        previousWatermark = eventTimestamp;
      }
    }

    return testStream.advanceWatermarkToInfinity();
  }

  @Test
  public void myTest() {
    final PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = TestPipeline.create(options);

    PCollection<KV<String, TestEvent>> input = p.apply(createTestStream());
    PCollection<List<TestEvent>> output = input.apply(ParDo.of(new SessionFn()));
    output.apply(ParDo.of(new PrintListDoFn<>()));
    p.run().waitUntilFinish();
  }
}
