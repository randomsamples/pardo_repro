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
  private static TestStream<KV<String,TestEvent>> createTestStream() {
    TestStream.Builder<KV<String,TestEvent>> testStream = TestStream.create(
        KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(TestEvent.class)));
    final Instant starTime = Instant.parse("2000-01-01T00:00:00Z");
    final Duration period = Duration.standardMinutes(5);
    final Duration gap = Duration.standardMinutes(30);

    List<KV<String,TestEvent>> testEvents = Arrays.asList(
        KV.of("Key1", new TestEvent("Key 1 - Session 1 - Event 1", starTime.plus(period.multipliedBy(0)))),

        KV.of("Key2", new TestEvent("Key 2 - Session 1 - Event 1", starTime.plus(period.multipliedBy(1)))),
        KV.of("Key2", new TestEvent("Key 2 - Session 1 - Event 2", starTime.plus(period.multipliedBy(2)))),

        null, // Add a watermark advancement larger than session gap, should split these sessions by firing the timers

        KV.of("Key1", new TestEvent("Key 1 - Session 2 - Event 1", starTime.plus(gap.plus(period.multipliedBy(3))))),
        KV.of("Key2", new TestEvent("Key 2 - Session 2 - Event 1", starTime.plus(gap.plus(period.multipliedBy(4))))),
        KV.of("Key1", new TestEvent("Key 1 - Session 2 - Event 2", starTime.plus(gap.plus(period.multipliedBy(5))))),
        KV.of("Key2", new TestEvent("Key 2 - Session 2 - Event 2", starTime.plus(gap.plus(period.multipliedBy(6))))),
        KV.of("Key2", new TestEvent("Key 2 - Session 2 - Event 3", starTime.plus(gap.plus(period.multipliedBy(7))))),

        null,

        KV.of("Key1", new TestEvent("Key 1 - Session 3 - Event 1", starTime.plus(gap.plus(period.multipliedBy(8))))),
        KV.of("Key1", new TestEvent("Key 1 - Session 3 - Event 2", starTime.plus(gap.plus(period.multipliedBy(9))))),
        KV.of("Key3", new TestEvent("Key 3 - Session 1 - Event 1", starTime.plus(gap.plus(period.multipliedBy(10))))),
        KV.of("Key2", new TestEvent("Key 2 - Session 3 - Event 1", starTime.plus(gap.plus(period.multipliedBy(11))))),
        KV.of("Key3", new TestEvent("Key 3 - Session 1 - Event 2", starTime.plus(gap.plus(period.multipliedBy(12))))),
        KV.of("Key2", new TestEvent("Key 2 - Session 3 - Event 2", starTime.plus(gap.plus(period.multipliedBy(13)))))
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
