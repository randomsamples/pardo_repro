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
import java.util.List;

public class SessionFnTest implements Serializable {
  private static TestStream<KV<String,TestEvent>> createTestStream() {
    TestStream.Builder<KV<String,TestEvent>> testStream = TestStream.create(
        KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(TestEvent.class)));
    final Instant starTime = Instant.parse("2000-01-01T00:00:00Z");
    final Duration period = Duration.standardMinutes(5);
    final Duration gap = Duration.standardMinutes(30);

    ImmutableList<KV<String,TestEvent>> testEvents = ImmutableList.of(
      KV.of("Key1", new TestEvent("Event 1", starTime)),
      KV.of("Key1", new TestEvent("Event 2", starTime.plus(period))),
      // Now, same key, but with a large event time gap
      KV.of("Key1", new TestEvent("Event 3", starTime.plus(gap.plus(period.multipliedBy(2))))),
      KV.of("Key1", new TestEvent("Event 4", starTime.plus(gap.plus(period.multipliedBy(3))))),
      KV.of("Key1", new TestEvent("Event 5", starTime.plus(gap.plus(period.multipliedBy(4)))))
    );

    for (KV<String,TestEvent> event : testEvents) {
      Instant eventTimestamp = event.getValue().getTimestamp();
      testStream = testStream.addElements(TimestampedValue.of(event, eventTimestamp));
      testStream = testStream.advanceWatermarkTo(eventTimestamp);
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
