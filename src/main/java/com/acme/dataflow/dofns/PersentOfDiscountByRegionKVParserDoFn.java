package com.acme.dataflow.dofns;

import com.acme.dataflow.model.RegionDiscount;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.values.KV;

/**
* 
*  csv line  => KV { currencycode -> discount }
*/

@Slf4j
public class PersentOfDiscountByRegionKVParserDoFn extends AbstractCSVKVEntityParser<RegionDiscount> {

    @ProcessElement
    @Override
    public void processElement(ProcessContext c) {
        String[] parts = pattern.split(c.element());
        if (parts.length < 2) {
            log.warn("bad record ", c.element());
        } else {
            // TODO : parseException
            c.output(KV.of(parts[0], RegionDiscount.of(parts[0], Double.parseDouble(parts[1]))));
        }
    }

}
