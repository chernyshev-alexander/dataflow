package com.acme.dataflow;

import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Rule;

@Slf4j
public class WindowsTest {

    @Rule
    public TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testFixedWindowed() {
        // generate seq [1,1,1,1,1,1, ..] with rate 2 events in second and calculate totals inside windows
        int RATE = 3;

        Window<Long> fixedWindow = Window.<Long>into(FixedWindows.of(Duration.standardSeconds(1)));

        PCollection<Long> ls = pipeline.apply(GenerateSequence.from(0L).to(100L)
                .withMaxReadTime(Duration.standardSeconds(5))
                .withRate(RATE, Duration.standardSeconds(1))) // generate RATE events in a second
                .apply(ParDo.of(new SequenceOfOnes()))
                .apply(fixedWindow)
                .apply(Combine.globally((Iterable<Long> it) -> {
                    return StreamSupport.stream(it.spliterator(), false).mapToLong(e -> e).sum();
                })
                .withoutDefaults());

        // for each window, counter should be between [RATE-2, RATE+2]
        PAssert.that(ls).satisfies((Iterable<Long> it) -> {
            it.forEach(eventsPerWindow -> {
                //log.debug("eventsPerWindow {}", eventsPerWindow);
                Assert.assertTrue(eventsPerWindow >= RATE - 2 && eventsPerWindow <= RATE + 2);
            });
            return null;
        });

        pipeline.run().waitUntilFinish();
    }

    static class SequenceOfOnes extends DoFn<Long, Long> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(1L);
        }
    }

}
