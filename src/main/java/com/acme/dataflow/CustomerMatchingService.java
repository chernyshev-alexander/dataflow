package com.acme.dataflow;

import lombok.extern.slf4j.Slf4j;
import java.util.Arrays;
import lombok.NonNull;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.repackaged.com.google.common.base.Optional;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import com.acme.dataflow.model.CustomerInfo;
import java.util.regex.Pattern;
import org.apache.beam.sdk.values.KV;

@Slf4j
public class CustomerMatchingService {
    
    static final String COMMA_SPLITTER_EXP = "\\s*,\\s*";

    public static class CustomerInfoCsvParser extends DoFn<String, CustomerInfo> {

        final Counter handledCustomerRecords = Metrics.counter(CustomerInfoCsvParser.class, "customer.parsed");

        @ProcessElement
        public void processElement(ProcessContext ctx) {

            String[] parts = ctx.element().split(COMMA_SPLITTER_EXP);

            CustomerInfo customerInfo = CustomerInfo.of(
                    Optional.absent(),
                    parts[0], parts[1],
                    parts[2], parts[3],
                    parts[4],
                    parts[5]);

            ctx.output(customerInfo);
            handledCustomerRecords.inc();

            log.debug(customerInfo.toString());
        }
    }

    public static class CustomerMatchingTransformer extends PTransform<PCollection<String>, PCollection<CustomerInfo>> {

        // main composite transformer
        @Override
        public PCollection<CustomerInfo> expand(PCollection<String> lines) {

            PCollection<CustomerInfo> customers = lines.apply(ParDo.of(new CustomerInfoCsvParser()));
            // here will be  complex pipeline 
            // TODO

            return customers;
        }
    }

    public static class CustomerCountryFilterDoFn extends DoFn<CustomerInfo, CustomerInfo> {

        // TODO : counters to display on UI
        private final Counter matched = Metrics.counter(CustomerCountryFilterDoFn.class, "matched.country");
        private final Counter unmatched = Metrics.counter(CustomerCountryFilterDoFn.class, "unmatched.country");

        @ProcessElement
        public void processElement(ProcessContext c) {

            CustomerInfo info = c.element();
            // here some stupid code just for example    
            if (Arrays.asList("USA", "POLAND").contains(info.country)) {
                log.debug("matched :" + info.country);
                matched.inc();
            } else {
                unmatched.inc();
                log.trace("unmatched :" + info.country);
            }
        }
    }

    // options from test or command line
    public interface Options extends PipelineOptions {

        @Description("source path")
        @Default.String("data/source.txt")
        public String getInputFile();

        public void setInputFile(String value);

        @Description("target path")
        @Default.String("data/result")
        @Required
        public String getOutput();

        public void setOutput(String value);
    }

    /**
     * Pipeline main entry
     *
     * @param options - pipeline arguments
     */
    public static void execute(@NonNull Options options) {

        Pipeline pipeline = Pipeline.create(options);

        PCollection<CustomerInfo> result
                = pipeline.apply("readlines", TextIO.read().from(options.getInputFile()))
                        .apply("customer.matching", new CustomerMatchingTransformer());
        // .apply(ParDo.of(new FilterDoFn(options.getFilterPattern())));

        // TODO : write result to google storage
        pipeline.run().waitUntilFinish();
    }

    public CustomerMatchingTransformer getMainTransformer() {
        return new CustomerMatchingTransformer();
    }

    public CustomerInfoCsvParser getCustomerParser() {
        return new CustomerInfoCsvParser();
    }

    public CountryCodesParser getCountryCodeParser() {
        return new CountryCodesParser(COMMA_SPLITTER_EXP);
    }

    public static void main(String[] args) {
        // TODO 
        Options options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .create()
                .as(Options.class);

        CustomerMatchingService.execute(options);
    }

}
