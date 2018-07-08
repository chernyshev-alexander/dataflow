
package com.acme.dataflow.dofns;

import com.acme.dataflow.model.CustomerInfo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * CustomerInfo ->  {CustomerInfo.countryCode -> CustomerInfo }
 * 
 * @author achernyshev
 */
public class KVCountryCodeCustomerDoFn extends DoFn<CustomerInfo, KV<String, CustomerInfo>> {

        @ProcessElement
        public void processElement(ProcessContext ctx) {
            ctx.output(KV.of(ctx.element().countryCode, ctx.element()));
        }
    };