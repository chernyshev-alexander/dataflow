package com.acme.dataflow.dofns;

import com.acme.dataflow.model.CustomerInfo;
import com.acme.dataflow.model.CustomerSales;
import com.acme.dataflow.model.SaleTx;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JoinerCustomersAndSalesDoFn extends DoFn<KV<String, CoGbkResult>, CustomerSales> {

    final TupleTag<SaleTx> saleTag;
    final TupleTag<CustomerInfo> customerTag;

    public JoinerCustomersAndSalesDoFn(TupleTag<CustomerInfo> customerTag, TupleTag<SaleTx> saleTag) {
        this.saleTag = saleTag;
        this.customerTag = customerTag;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {

        KV<String, CoGbkResult> kv = c.element();
        
        String key = kv.getKey();
        CoGbkResult joinResult = kv.getValue();
        
        CustomerInfo customer = joinResult.getOnly(customerTag, null);
        
        if (customer == null) {
         // don't write output when sale doesn't have customer,  [] -> [sales, ..]
         // could be if we have inconsistent data
            return ;
        }
        
        // [customer] -> [sales, ..]
        CustomerSales result = CustomerSales.of(customer);
        joinResult.getAll(saleTag).forEach(e -> result.getSales().add(e));
 
        c.output(result);
    }
}
