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
import com.acme.dataflow.dofns.SplitCustomersByRegionDoFn;
import com.acme.dataflow.model.CustomerInfo;
import com.acme.dataflow.dofns.CountryCodesParser;
import com.acme.dataflow.dofns.CustomerInfoCsvParserDoFn;
import com.acme.dataflow.dofns.EnrichCustomerWithCountryFn;
import com.acme.dataflow.dofns.JoinerCustomersAndSalesDoFn;
import com.acme.dataflow.dofns.CountryCodeKeyForCustomerDoFn;
import com.acme.dataflow.dofns.CustomerSalesStoresDiscounterDoFn;
import com.acme.dataflow.dofns.PhoneNameKeyForCustomerDoFn;
import com.acme.dataflow.dofns.RegionalDiscountKVParserDoFn;
import com.acme.dataflow.dofns.SalesKVParserDoFn;
import com.acme.dataflow.dofns.StoresKVParserDoFn;
import com.acme.dataflow.model.CustomerSales;
import com.acme.dataflow.model.RegionalDiscount;
import com.acme.dataflow.model.SaleTx;
import com.acme.dataflow.model.Store;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;

@Slf4j
public class CustomerMatchingService {

    public static final String TAG_EU_REGION = "EU-";
    public static final String TAG_US_REGION = "US-";
    public static final String TAG_UNKNOWN_REGION = "UNDEF-";

    public static final TupleTag<CustomerSales> EU_SALES = new TupleTag<>();
    public static final TupleTag<CustomerSales> US_SALES = new TupleTag<>();
    public static final TupleTag<CustomerSales> UNDEF_SALES = new TupleTag<>();

    final Pipeline pipeline;
    final Options options;

    public CustomerMatchingService(Pipeline pipeline, Options options) {
        this.pipeline = pipeline;
        this.options = options;
    }

    // parsers CSV -> KV<Primary Key, Entity>
    public PCollection<KV<String, String>> readCountryCodes(PTransform<PBegin, PCollection<String>> reader) {
        return pipeline.apply(CountryCodesParser.class.getName(), reader)
                .apply(ParDo.of(new CountryCodesParser()));
    }

    public PCollection<CustomerInfo> readCustomers(PTransform<PBegin, PCollection<String>> reader) {
        return pipeline.apply(CustomerInfoCsvParserDoFn.class.getName(), reader)
                .apply(ParDo.of(new CustomerInfoCsvParserDoFn()));
    }

    public PCollection<KV<String, CustomerInfo>> readCustomersWithPK(PTransform<PBegin, PCollection<String>> reader) {
        return readCustomers(reader).apply(ParDo.of(new PhoneNameKeyForCustomerDoFn()));
    }

    public PCollection<KV<String, SaleTx>> readSalesWithPK(PTransform<PBegin, PCollection<String>> reader) {
        return pipeline.apply(SalesKVParserDoFn.class.getName(), reader)
                .apply(ParDo.of(new SalesKVParserDoFn()));
    }

    public PCollection<KV<String, Store>> readStoresWithPK(PTransform<PBegin, PCollection<String>> reader) {
        return pipeline.apply(StoresKVParserDoFn.class.getName(), reader)
                .apply(ParDo.of(new StoresKVParserDoFn()));
    }

    public PCollection<KV<String, RegionalDiscount>> readRegionalDiscountsWithPK(PTransform<PBegin, PCollection<String>> reader) {
        return pipeline.apply(RegionalDiscountKVParserDoFn.class.getName(), reader)
                .apply(ParDo.of(new RegionalDiscountKVParserDoFn()));
    }

    // End of CSV parsers
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

        PCollection<KV<String, CustomerInfo>> keyedCustomers = customers.apply(ParDo.of(new CountryCodeKeyForCustomerDoFn()));

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
    public PCollectionTuple splitCustomersByRegion(final PCollection<CustomerInfo> allCustomers) {
        return allCustomers
                .apply(ParDo.of(new SplitCustomersByRegionDoFn())
                        .withOutputTags(SplitCustomersByRegionDoFn.TAG_EU_CUSTOMER,
                                TupleTagList.of(SplitCustomersByRegionDoFn.TAG_UDEF_COUNTRY_CUSTOMER)
                                        .and(SplitCustomersByRegionDoFn.TAG_USA_CUSTOMER)));
    }

    /**
     *
     * Split pipeline by regions, join with sales for each region and merge back
     *
     * customers_by_regions = splitCustomersByRegion(customers); [EU, US, OTHER]
     *
     * for (c <- customers_by_regions) yield {
     *      select customers -> [sales, ..] from customers inner join sales on customers.(phone;last_name) ==
     * sales.(phone;last_name) } collect to tuple (eu_sales, us_sales, others_sales)
     *
     * @param customers - all customers
     * @param sales - all sales
     * @param stores - all stores
     * @param discount - all discounts by regions
     *
     * @return 3 collections for each region (EU, US, OTHER)
     *
     */
    public PCollectionTuple makeSalesReportByVendorsWithRegionalDiscount(
            final PCollection<CustomerInfo> customers,
            final PCollection<KV<String, SaleTx>> sales,
            final PCollection<KV<String, Store>> stores,
            final PCollection<KV<String, RegionalDiscount>> discount) {

        PCollectionTuple customersByRegion = splitCustomersByRegion(customers);

        PCollection<CustomerInfo> euCustomers = customersByRegion.get(SplitCustomersByRegionDoFn.TAG_EU_CUSTOMER);
        PCollection<CustomerInfo> usCustomers = customersByRegion.get(SplitCustomersByRegionDoFn.TAG_USA_CUSTOMER);
        PCollection<CustomerInfo> undefCustomers = customersByRegion.get(SplitCustomersByRegionDoFn.TAG_UDEF_COUNTRY_CUSTOMER);

        PCollection<CustomerSales> euCustomerSales = joinCustomersWithSales(TAG_EU_REGION, euCustomers, sales);
        PCollection<CustomerSales> usCustomerSales = joinCustomersWithSales(TAG_US_REGION, usCustomers, sales);
        PCollection<CustomerSales> undefCustomerSales = joinCustomersWithSales(TAG_UNKNOWN_REGION, undefCustomers, sales);

        // create dictionaries with unique names from short pcollections for side inputs
        PCollectionView<Map<String, Store>> storesView = stores.apply("stores", View.asMap());
        PCollectionView<Map<String, RegionalDiscount>> discountView = discount.apply("discounts", View.asMap());

        PCollection<CustomerSales> euCustomerSalesDiscounted
                = applyDiscountsForStoresInRegion(euCustomerSales, storesView, discountView);
        PCollection<CustomerSales> usCustomerSalesDiscounted
                = applyDiscountsForStoresInRegion(usCustomerSales, storesView, discountView);
        PCollection<CustomerSales> undefCustomerSalesDiscounted
                = applyDiscountsForStoresInRegion(undefCustomerSales, storesView, discountView);

        return PCollectionTuple.of(EU_SALES, euCustomerSalesDiscounted)
                .and(US_SALES, usCustomerSalesDiscounted)
                .and(UNDEF_SALES, undefCustomerSalesDiscounted);
    }

    private PCollection<CustomerSales> applyDiscountsForStoresInRegion(
            final PCollection<CustomerSales> customerSales,
            final PCollectionView<Map<String, Store>> storesView,
            final PCollectionView<Map<String, RegionalDiscount>> discountView) {

        log.debug("apply discounts in stores for region {}", customerSales.getName());

        return customerSales.apply(customerSales.getName() + "store-discount",
                ParDo.of(new CustomerSalesStoresDiscounterDoFn(storesView, discountView))
                        .withSideInputs(storesView)
                        .withSideInputs(discountView));
    }

    /**
     *
     * @param regionName - to provide uniqueness of transformers names
     * @param customers - customers
     * @param sales - sales
     * @return CustomerSales - [customer] - 1:* [sales] inner join on (phone, customer_name)
     */
    private PCollection<CustomerSales> joinCustomersWithSales(
            String regionName,
            final PCollection<CustomerInfo> customers,
            final PCollection<KV<String, SaleTx>> sales) {

        final TupleTag<CustomerInfo> tagCustomerInfo = new TupleTag<>();
        final TupleTag<SaleTx> tagSaleTx = new TupleTag<>();

        PCollection<KV<String, CustomerInfo>> keyPhoneNameCustomer
                = customers.apply(regionName, ParDo.of(new PhoneNameKeyForCustomerDoFn()));

        PCollection<KV<String, CoGbkResult>> joinSales = KeyedPCollectionTuple
                .of(tagCustomerInfo, keyPhoneNameCustomer)
                .and(tagSaleTx, sales)
                .apply(regionName + "JoinSalesCoGroup", CoGroupByKey.<String>create());

        PCollection<CustomerSales> customerSales = joinSales.apply(regionName + "CustomerSales",
                ParDo.of(new JoinerCustomersAndSalesDoFn(tagCustomerInfo, tagSaleTx)));

        customerSales.setName(regionName);

        return customerSales;
    }

    /**
     * Merge sales by regions to one report
     * 
     * @param customerSalesTuple - tuple with 3 p-collection for each region
     * @return merged version of the p-collection
     */
    PCollection<CustomerSales> mergeAllCustomerSalesToOneStream(PCollectionTuple customerSalesTuple) {

        PCollection<CustomerSales> euCustomers = customerSalesTuple.get(CustomerMatchingService.EU_SALES);
        PCollection<CustomerSales> usCustomers = customerSalesTuple.get(CustomerMatchingService.US_SALES);
        PCollection<CustomerSales> undefCustomers = customerSalesTuple.get(CustomerMatchingService.UNDEF_SALES);
        
        List<PCollection<CustomerSales>> tabs = Arrays.asList(euCustomers, usCustomers, undefCustomers);
        
        return PCollectionList.of(tabs).apply(Flatten.<CustomerSales>pCollections());
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
