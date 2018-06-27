package com.acme.dataflow;

import java.util.regex.Pattern;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class CountryCodesParser extends DoFn<String, KV<String, String>> {

    final Pattern pattern;

    public CountryCodesParser(String regExp) {
        pattern = Pattern.compile(regExp);

    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        String[] parts = pattern.split(c.element());
        c.output(KV.of(parts[0], parts[1]));
    }
}
