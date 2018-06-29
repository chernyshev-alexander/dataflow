package com.acme.dataflow;

import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

@Slf4j
public class CountryCodesParser extends DoFn<String, KV<String, String>> {

    final Pattern pattern;

    public CountryCodesParser(String regExp) {
        pattern = Pattern.compile(regExp);
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        String[] parts = pattern.split(c.element());
        if (parts.length < 2) {
            log.warn("bad record ", c.element());
        } else {
            c.output(KV.of(parts[0], parts[1]));
        }
    }
}
