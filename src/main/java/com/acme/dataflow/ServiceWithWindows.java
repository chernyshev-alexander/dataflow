package com.acme.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

public class ServiceWithWindows {

    final Pipeline pipeline;

    public ServiceWithWindows(Pipeline pipeline) {
        this.pipeline = pipeline;
    }

    public PCollection<Integer> readStream(PTransform<PBegin, PCollection<Integer>> reader) {
        return pipeline.apply(reader);
    }

    public PCollection<Integer> fixedWindowed(PCollection<Integer> ls) {
        return ls.apply(Window.into(FixedWindows.of(Duration.standardSeconds(1))));
    }    
    
}
