package com.acme.dataflow.dofns;

import com.acme.dataflow.model.CustomerInfo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.repackaged.com.google.common.base.Optional;
import java.util.regex.Pattern;

public class CustomerInfoCsvParserDoFn extends DoFn<String, CustomerInfo> {

    static final String COMMA_SPLITTER_EXP = "\\s*,\\s*";

    final Counter handledCustomerRecords = Metrics.counter(CustomerInfoCsvParserDoFn.class, "customer.parsed");
    final Pattern pattern;

    public CustomerInfoCsvParserDoFn(String regExp) {
        pattern = Pattern.compile(regExp);
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) {

        String[] parts = ctx.element().split(COMMA_SPLITTER_EXP);
        CustomerInfo customerInfo = CustomerInfo.of(
                Optional.absent(),
                parts[0], parts[1],
                parts[2], parts[3],
                parts[4],
                parts[5], null);

        ctx.output(customerInfo);
        
        handledCustomerRecords.inc();
    }
}
