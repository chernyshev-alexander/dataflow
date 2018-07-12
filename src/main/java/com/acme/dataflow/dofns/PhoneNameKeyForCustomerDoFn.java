package com.acme.dataflow.dofns;

import com.acme.dataflow.model.CustomerInfo;
import org.apache.beam.sdk.repackaged.org.apache.commons.lang3.StringUtils;
import static org.apache.beam.sdk.repackaged.org.apache.commons.lang3.StringUtils.SPACE;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

//
// customer => {(phoneNo, lastName) -> CustomerInfo }
//
public class PhoneNameKeyForCustomerDoFn extends DoFn<CustomerInfo, KV<String, CustomerInfo>> {

        @ProcessElement
        public void processElement(ProcessContext ctx) {
            
            CustomerInfo info = ctx.element();
            
            String normalizedPhoneNo = StringUtils.remove(info.phoneNo, SPACE);
            
            String key = StringUtils.joinWith(";", normalizedPhoneNo, info.lastName);
           
            ctx.output(KV.of(key, info));
        }
    };
    
