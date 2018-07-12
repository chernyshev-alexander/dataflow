package com.acme.dataflow.dofns;

import com.acme.dataflow.model.Store;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
*  csv line  => KV { storeId -> Store }
*/

@Slf4j
public class StoresKVParserDoFn extends DoFn<String, KV<String, Store>> implements CSVCommons {
    
    @ProcessElement
    public void processElement(ProcessContext c) {
        
        String[] parts = pattern.split(c.element());
        
        if (parts.length < 2) {
            log.warn("bad record ", c.element());
        } else {
            c.output(KV.of(parts[0], Store.of(parts[0], parts[1])));
        }
    }
    
}
