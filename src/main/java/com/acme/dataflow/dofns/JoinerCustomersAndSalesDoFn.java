package com.acme.dataflow.dofns;

import com.acme.dataflow.model.CustomerInfo;
import com.acme.dataflow.model.CustomerSales;
import com.acme.dataflow.model.SaleTx;
import org.apache.beam.sdk.repackaged.org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

public class JoinerCustomersAndSalesDoFn extends DoFn<KV<ImmutablePair<String, String>, CoGbkResult>, CustomerSales> {

    final TupleTag<SaleTx> saleTag;
    final TupleTag<CustomerInfo> customerTag;

    public JoinerCustomersAndSalesDoFn(TupleTag<CustomerInfo> customerTag, TupleTag<SaleTx> saleTag) {
        this.saleTag = saleTag;
        this.customerTag = customerTag;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {

        KV<ImmutablePair<String, String>, CoGbkResult> kv = c.element();
        
        ImmutablePair<String, String> k = kv.getKey();
        
        CustomerInfo customer = kv.getValue().getOnly(customerTag);
        Iterable<SaleTx> sales = kv.getValue().getAll(saleTag);
        
        CustomerSales cs  = CustomerSales.of(customer);
                
        sales.forEach((SaleTx tx) -> cs.getSales().add(tx));
        
        c.output(cs);
    }
}
