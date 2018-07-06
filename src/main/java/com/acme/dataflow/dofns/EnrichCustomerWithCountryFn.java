package com.acme.dataflow.dofns;

import com.acme.dataflow.model.CustomerInfo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

public class EnrichCustomerWithCountryFn extends DoFn<KV<String, CoGbkResult>, CustomerInfo> {

    final TupleTag<String> countryTag;
    final TupleTag<CustomerInfo> customerTag;

    public EnrichCustomerWithCountryFn(TupleTag<String> countryTag, TupleTag<CustomerInfo> customerTag) {
        this.countryTag = countryTag;
        this.customerTag = customerTag;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {

        KV<String, CoGbkResult> kv = c.element();
        String countryName = kv.getValue().getOnly(countryTag);

        Iterable<CustomerInfo> it = kv.getValue().getAll(customerTag);
        it.forEach((CustomerInfo customer) -> {
            // create a fresh copy of customer and set up countryName field and 
            // emit a new copy of customer
            // modify current copy of customer isn't allowed by dataflow !!
            c.output(customer.withCountryName(countryName));
        });
    }
}
