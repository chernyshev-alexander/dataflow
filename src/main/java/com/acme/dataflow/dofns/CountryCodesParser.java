package com.acme.dataflow.dofns;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.KV;

@Slf4j
public class CountryCodesParser extends DoFn<String, KV<String, String>> implements CSVParsers {

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
