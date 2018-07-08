package com.acme.dataflow.dofns;

import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.values.KV;

@Slf4j
public class CountryCodesParser extends AbstractCSVKVEntityParser<String> {

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
