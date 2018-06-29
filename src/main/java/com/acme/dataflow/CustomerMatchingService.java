package com.acme.dataflow;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
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
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.TupleTag;

@Slf4j
public class CustomerMatchingService {

    final Pipeline pipeline;
    final Options options;

    static final String COMMA_SPLITTER_EXP = "\\s*,\\s*";

    public static class CustomerInfoCsvParser extends DoFn<String, CustomerInfo> {

        final Counter handledCustomerRecords = Metrics.counter(CustomerInfoCsvParser.class, "customer.parsed");
        final Pattern pattern;

        public CustomerInfoCsvParser(String regExp) {
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

    public static class CustomerMatchingTransformer extends PTransform<PCollection<String>, PCollection<CustomerInfo>> {

        @Override
        public PCollection<CustomerInfo> expand(PCollection<String> customerLines) {
            return null;
        }
    }

    public static class EnrichCustomerWithCountryFn extends DoFn<KV<String, CoGbkResult>, CustomerInfo> {

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

    public CustomerMatchingService(Pipeline pipeline, Options options) {
        this.pipeline = pipeline;
        this.options = options;
    }

    public PCollection<CustomerInfo> readCustomers(PTransform<PBegin, PCollection<String>> reader) {
        return pipeline.apply("ReadCustomersCSV", reader)
                .apply(ParDo.of(new CustomerInfoCsvParser(COMMA_SPLITTER_EXP)));
    }

    /**
     * Transform countries strings to key value p-collection
     *
     * example : "POL, POLAND" -> KV{"POL", "POLAND"}
     *
     * @param reader - source of strings (java list or BigQuery reader)
     * @return PCollection of KV(countryCode, countryName)
     */
    public PCollection<KV<String, String>> readCountryCodes(PTransform<PBegin, PCollection<String>> reader) {
        return pipeline.apply("ReadCSVCountryCodes", reader)
                .apply(ParDo.of(new CountryCodesParser(COMMA_SPLITTER_EXP)));
    }

    /**
     * Convert CustomerInfo -> KV<CustomerInfo.countryCode, CustomerInfo>
     */
    public static class KeyedCustomerDoFn extends DoFn<CustomerInfo, KV<String, CustomerInfo>> {

        @ProcessElement
        public void processElement(ProcessContext ctx) {
            ctx.output(KV.of(ctx.element().countryCode, ctx.element()));
        }
    };

    /**
     * Implements left join customer with country by countryCode as :
     *
     * <code>
     *  select customer{countryName = country.name}
     *    from customer
     *    left join country on customer.countryCode = country.countryCode
     * </code>
     * 
     * @param customers  p-collection
     * @param countries  p-collections of KV{countryCode, countryName}
     * @return enriched with countryName p-collection of the customers
     */
    public PCollection<CustomerInfo> enrichCustomerWithCountryCode(
            final PCollection<CustomerInfo> customers,
            final PCollection<KV<String, String>> countries) {

        final TupleTag<CustomerInfo> customerTag = new TupleTag<>();
        final TupleTag<String> countryTag = new TupleTag<>();

        PCollection<KV<String, CustomerInfo>> keyedCustomers = customers.apply(ParDo.of(new KeyedCustomerDoFn()));

        PCollection<KV<String, CoGbkResult>> customerWithCountryJoin = KeyedPCollectionTuple
                .of(customerTag, keyedCustomers)
                .and(countryTag, countries)
                .apply(CoGroupByKey.create());

        PCollection<CustomerInfo> result = customerWithCountryJoin.apply("EnrichCustomerwithCountry",
                ParDo.of(new EnrichCustomerWithCountryFn(countryTag, customerTag)));

        return result;
    }

    public void execute() {

        //PCollection<CustomerInfo> customers = enrichCustomerWithCountryCode(
        //        readCustomers(TextIO.read().from(options.getCustomersSource())),
        //        readCountryCodes(TextIO.read().from(options.getCountriesSource())));

        //PCollection<CustomerInfo> result = csvCustomers.apply("CSV2Customer", new CustomerMatchingTransformer());
        // TODO : write result to google storage
        pipeline.run().waitUntilFinish();
    }

    // pipeline options from test or command line
    public interface Options extends PipelineOptions {

        @Description("customers")
        @Default.String("data/customers.csv")
        public String getCustomersSource();

        public void setCustomersSource(String value);

        @Description("countries")
        @Default.String("data/countries.csv")
        public String getCountriesSource();

        public void setCountriesSource(String value);

        @Description("target path")
        @Default.String("data/result")
        @Required
        public String getOutput();

        public void setOutput(String value);
    }

    public static void main(String[] args) {
        // TODO 
        Options options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .create()
                .as(Options.class);

        new CustomerMatchingService(Pipeline.create(options), options).execute();
    }

}
