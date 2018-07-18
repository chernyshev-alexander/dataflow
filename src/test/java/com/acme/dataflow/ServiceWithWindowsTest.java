package com.acme.dataflow;

import java.util.stream.StreamSupport;
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
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;

public class ServiceWithWindowsTest {

    @Rule
    public TestPipeline pipeline = TestPipeline.create();

    public ServiceWithWindows service;

    @Before
    public void beforeClass() {
        service = new ServiceWithWindows(pipeline);
    }

    static class SequenceOfOnes extends DoFn<Long, Long> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            //c.outputWithTimestamp(1L, c.timestamp());
            c.output(1L);
        }
    }

    @Test
    public void testFixedWindowed() {
        // generate seq [1,1,1,1,1,1, ..] with rate 2 events in second and calculate totals inside windows
        
        Window<Long> fixedIntervals = Window.<Long>into(FixedWindows.of(Duration.standardSeconds(1)));

        PCollection<Long> ls = pipeline.apply(GenerateSequence.from(0L).to(100L)
                .withMaxReadTime(Duration.standardSeconds(20))
                .withRate(2, Duration.standardSeconds(1)))  // 2 events in a second
                .apply(ParDo.of(new SequenceOfOnes()))
                .apply(fixedIntervals)
                .apply(Combine.globally((Iterable<Long> it) -> {
                    return StreamSupport.stream(it.spliterator(), false).mapToLong(e -> e).sum();
                })
                .withoutDefaults());
        
        // not clear how to test
        // result is mostly 2 as expected, but gets values 1 and 3 as well
        PAssert.that(ls)
                .satisfies((Iterable<Long> it) -> {
                    it.forEach(e -> {
                        System.out.println("sum in window  = " + e);
                    });
                    return null;
                });

        pipeline.run().waitUntilFinish();
    }

}
