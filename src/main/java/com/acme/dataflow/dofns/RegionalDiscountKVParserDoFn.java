package com.acme.dataflow.dofns;

import com.acme.dataflow.model.RegionalDiscount;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.KV;

/**
*  csv line  => KV { currencycode -> RegionDiscount }
*/

@Slf4j
public class RegionalDiscountKVParserDoFn extends DoFn<String, KV<String, RegionalDiscount>> implements CSVCommons {

    @ProcessElement
    public void processElement(ProcessContext ctx) {
        
        String[] parts = pattern.split(ctx.element());
        
        if (parts.length < 2) {
            log.warn("bad record ", ctx.element());
        } else {
            ctx.output(KV.of(parts[0], RegionalDiscount.of(parts[0], Double.parseDouble(parts[1]))));
        }
    }

}
