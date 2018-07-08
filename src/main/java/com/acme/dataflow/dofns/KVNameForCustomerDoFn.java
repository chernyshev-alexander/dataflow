package com.acme.dataflow.dofns;

import com.acme.dataflow.model.CustomerInfo;
import org.apache.beam.sdk.repackaged.org.apache.commons.lang3.StringUtils;
import static org.apache.beam.sdk.repackaged.org.apache.commons.lang3.StringUtils.SPACE;
import org.apache.beam.sdk.repackaged.org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

//
// customer => {(phoneNo, lastName) -> CustomerInfo }
//
public class KVNameForCustomerDoFn extends DoFn<CustomerInfo, 
        KV<ImmutablePair<String, String>, CustomerInfo>> {

        @ProcessElement
        public void processElement(ProcessContext ctx) {
            
            CustomerInfo info = ctx.element();
            
            String normalizedPhoneNo = StringUtils.remove(info.phoneNo, SPACE);
            
            ImmutablePair<String, String> pair = 
                    new ImmutablePair<>(normalizedPhoneNo, info.lastName);
           
            ctx.output(KV.of(pair, info));
        }
    };
    
