package com.acme.dataflow;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import com.acme.dataflow.dofns.SplitCustomerByCountryDoFn;
import com.acme.dataflow.model.CustomerInfo;
import com.acme.dataflow.dofns.CountryCodesParser;
import com.acme.dataflow.dofns.CustomerInfoCsvParserDoFn;
import com.acme.dataflow.dofns.EnrichCustomerWithCountryFn;
import com.acme.dataflow.dofns.JoinerCustomersAndSalesDoFn;
import com.acme.dataflow.dofns.KVCountryCodeCustomerDoFn;
import com.acme.dataflow.dofns.KVNameForCustomerDoFn;
import com.acme.dataflow.dofns.SalesKVParserDoFn;
import com.acme.dataflow.model.SaleTx;
import org.apache.beam.sdk.repackaged.org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.beam.sdk.transforms.DoFn;

@Slf4j
public class CustomerMatchingService {

    static final String COMMA_SPLITTER_EXP = "\\s*,\\s*";

    final Pipeline pipeline;
    final Options options;

    public CustomerMatchingService(Pipeline pipeline, Options options) {
        this.pipeline = pipeline;
        this.options = options;
    }

    public PCollection<CustomerInfo> readCustomers(PTransform<PBegin, PCollection<String>> reader) {
        return pipeline.apply("ReadCustomersCSV", reader)
                .apply(ParDo.of(new CustomerInfoCsvParserDoFn(COMMA_SPLITTER_EXP)));
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
                .apply(ParDo.of(new CountryCodesParser()));
    }

    /**
     * Implements left join customer with country by countryCode as :
     *
     * <code>
     *  select customer{countryName = country.name}
     *    from customer
     *    left join country on customer.countryCode = country.countryCode
     * </code>
     *
     * @param customers p-collection
     * @param countries p-collections of KV{countryCode, countryName}
     * @return enriched with countryName p-collection of the customers
     */
    public PCollection<CustomerInfo> enrichCustomerWithCountryCode(
            final PCollection<CustomerInfo> customers,
            final PCollection<KV<String, String>> countries) {

        final TupleTag<CustomerInfo> customerTag = new TupleTag<>();
        final TupleTag<String> countryTag = new TupleTag<>();

        PCollection<KV<String, CustomerInfo>> keyedCustomers = customers.apply(ParDo.of(new KVCountryCodeCustomerDoFn()));

        PCollection<KV<String, CoGbkResult>> customerWithCountryJoin = KeyedPCollectionTuple
                .of(customerTag, keyedCustomers)
                .and(countryTag, countries)
                .apply(CoGroupByKey.create());

        PCollection<CustomerInfo> result = customerWithCountryJoin.apply("EnrichCustomerwithCountry",
                ParDo.of(new EnrichCustomerWithCountryFn(countryTag, customerTag)));

        return result;
    }

    /**
     * (customers) -> (customers EU or UK), (customers US), (customers Others)
     *
     * @param allCustomers - customers
     * @return 3 collections are branched by TAG_**
     */
    public PCollectionTuple branchCustomersByCountryCode(final PCollection<CustomerInfo> allCustomers) {
        return allCustomers
                .apply(ParDo.of(new SplitCustomerByCountryDoFn())
                        .withOutputTags(SplitCustomerByCountryDoFn.TAG_EU_CUSTOMER,
                                TupleTagList.of(SplitCustomerByCountryDoFn.TAG_UDEF_COUNTRY_CUSTOMER)
                                        .and(SplitCustomerByCountryDoFn.TAG_USA_CUSTOMER)));
    }

    public PCollection<String> matchSalesWithCustomers(
                    final PCollection<CustomerInfo> allCustomers,
                    final PCollection<KV<ImmutablePair<String, String>, SaleTx>> sales) {
        
        PCollectionTuple countryCustomers = branchCustomersByCountryCode(allCustomers);
        
        PCollection<CustomerInfo> euCustomers = countryCustomers.get(SplitCustomerByCountryDoFn.TAG_EU_CUSTOMER);
        PCollection<CustomerInfo> usCustomers = countryCustomers.get(SplitCustomerByCountryDoFn.TAG_USA_CUSTOMER);
        PCollection<CustomerInfo> undefCustomers = countryCustomers.get(SplitCustomerByCountryDoFn.TAG_UDEF_COUNTRY_CUSTOMER);
        
        final TupleTag<CustomerInfo> tagCustomerInfo = new TupleTag<>();
        final TupleTag<SaleTx> tagSaleTx = new TupleTag<>();

        PCollection<KV<ImmutablePair<String, String>, CustomerInfo>> keyPhoneNameCustomer = 
                euCustomers.apply(ParDo.of(new KVNameForCustomerDoFn()));
            
        // join customer as c, sales as s where c.phone = s.phone and c.lastname = s.customername
        PCollection<KV<ImmutablePair<String, String>, CoGbkResult>> join = KeyedPCollectionTuple
                .of(tagCustomerInfo, keyPhoneNameCustomer)
                .and(tagSaleTx, sales)
                .apply(CoGroupByKey.create());
        
        PCollection<String> result = join.apply("JoinCustomersAndSales", 
                ParDo.of(new JoinerCustomersAndSalesDoFn(tagCustomerInfo, tagSaleTx)));

        return result;
 
    }
    
    /**
     * main logic for pipeline
     */
    public void execute() {

        // TODO
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
                .as(Options.class
                );

        //new CustomerMatchingService(Pipeline.create(options), options).execute();
    }

}
