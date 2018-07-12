package com.acme.dataflow;

import java.util.Arrays;
import java.util.List;
import com.acme.dataflow.dofns.SplitCustomersByRegionDoFn;
import com.acme.dataflow.model.CustomerInfo;
import com.acme.dataflow.model.CustomerSales;
import com.acme.dataflow.model.SaleTx;
import java.util.Collections;
import org.apache.beam.runners.direct.repackaged.com.google.common.base.Strings;
import org.apache.beam.sdk.repackaged.com.google.common.base.Function;
import org.apache.beam.sdk.repackaged.com.google.common.base.Optional;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class CustomerMatchingServiceTest {

    @Rule
    public TestPipeline pipeline = TestPipeline.create();
    CustomerMatchingService.Options options;

    @Before
    public void before() {
        options = TestPipeline.testingPipelineOptions().as(CustomerMatchingService.Options.class);
    }

    @Test
    public void testReadSalesWithPK() {

        CustomerMatchingService service = new CustomerMatchingService(pipeline, options);
        PCollection<KV<String, SaleTx>> ls = service.readSalesWithPK(Create.of(sales));

        List<String> keys = Arrays.asList("+11100001999;IVANOVA", "+48516420276;CHERNYSHEV", "+11111111111;DONALN");

        SerializableFunction<Iterable<KV<String, SaleTx>>, Void> sf = it -> {
            it.forEach(e -> assertTrue(keys.contains(e.getKey())));
            return null;
        };

        PAssert.that(ls).satisfies(sf);
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testCustomerParser() {

        CustomerMatchingService service = new CustomerMatchingService(pipeline, options);
        PCollection<CustomerInfo> ls = service.readCustomers(Create.of(CustomerMatchingServiceTest.customers));

        PAssert.that(ls).containsInAnyOrder(customersExpected);
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void readCustomersWithPK() {

        CustomerMatchingService service = new CustomerMatchingService(pipeline, options);

        PCollection<KV<String, CustomerInfo>> ls = service.readCustomersWithPK(Create.of(customers));

        List<String> keys = Arrays.asList("+11100001999;IVANOVA", "+48516420276;CHERNYSHEV");

        SerializableFunction<Iterable<KV<String, CustomerInfo>>, Void> sf = it -> {
            it.forEach(e -> assertTrue(keys.contains(e.getKey())));
            return null;
        };

        PAssert.that(ls).satisfies(sf);
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testEnrichCustomerWithCountryName() {

        CustomerMatchingService service = new CustomerMatchingService(pipeline, options);

        PCollection<CustomerInfo> customers = service.readCustomers(Create.of(CustomerMatchingServiceTest.customers));
        PCollection<KV<String, String>> countries = service.readCountryCodes(Create.of(countryCodes));

        // CustomerInfo.countryCode should be one of the countries codes POL, US, UK ..
        PCollection<CustomerInfo> enrichedCustomers = service.enrichCustomerWithCountryCode(customers, countries);

        SerializableFunction<Iterable<CustomerInfo>, Void> sf = (Iterable<CustomerInfo> it) -> {
            it.forEach(e -> assertTrue(!Strings.isNullOrEmpty(e.getCountryName())));
            return null;
        };

        PAssert.that(enrichedCustomers).satisfies(sf);
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testSplitCustomersByRegion() {

        CustomerMatchingService service = new CustomerMatchingService(pipeline, options);

        PCollection<CustomerInfo> ls = service.readCustomers(Create.of(CustomerMatchingServiceTest.customers));

        PCollectionTuple result = service.splitCustomersByRegion(ls);

        PCollection<CustomerInfo> euCustomers = result.get(SplitCustomersByRegionDoFn.TAG_EU_CUSTOMER);
        PCollection<CustomerInfo> usCustomers = result.get(SplitCustomersByRegionDoFn.TAG_USA_CUSTOMER);
        PCollection<CustomerInfo> undefCustomers = result.get(SplitCustomersByRegionDoFn.TAG_UDEF_COUNTRY_CUSTOMER);

        Function<List<String>, SerializableFunction<Iterable<CustomerInfo>, Void>> testFun
                = (List<String> codes) -> (Iterable<CustomerInfo> it) -> {
                    it.forEach((CustomerInfo e) -> assertTrue(codes.contains(e.countryCode)));
                    return null;
                };

        // test that each collection has corresponded country codes
        PAssert.that(euCustomers).satisfies(testFun.apply(Arrays.asList("POL", "UK")));
        PAssert.that(usCustomers).satisfies(testFun.apply(Arrays.asList("US")));
        PAssert.that(undefCustomers).satisfies(testFun.apply(Collections.EMPTY_LIST));

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testMakeSalesReportByVendorsWithRegionalDiscounts() {

        CustomerMatchingService service = new CustomerMatchingService(pipeline, options);

        PCollection<CustomerInfo> rawCustomers = service.readCustomers(Create.of(customers));
        PCollection<KV<String, String>> countries = service.readCountryCodes(Create.of(countryCodes));

        PCollection<CustomerInfo> ls_customers = service.enrichCustomerWithCountryCode(rawCustomers, countries);

        PCollectionTuple customerSales
                = service.makeSalesReportByVendorsWithRegionalDiscount(
                        ls_customers,
                        service.readSalesWithPK(Create.of(sales)),
                        service.readStoresWithPK(Create.of(stores)),
                        service.readRegionalDiscountsWithPK(Create.of(regionalDiscounts)));

        // common test function
        Function<List<String>, SerializableFunction<Iterable<CustomerSales>, Void>> testFun
                = (List<String> ls) -> (Iterable<CustomerSales> it) -> {
                    it.forEach(e -> {
                        assertTrue(e.getCustomer().getLastName().equalsIgnoreCase(ls.get(0)));
                        e.getSales().forEach(s -> assertTrue(s.getCurrencyCode().equalsIgnoreCase(ls.get(1))));
                    });
                    return null;
                };

        // customers from EU payed in EUR
        PAssert.that(customerSales.get(CustomerMatchingService.EU_SALES))
                             .satisfies(testFun.apply(Arrays.asList("CHERNYSHEV", "EUR")));
        // customers from USA payed in EUR as well
        PAssert.that(customerSales.get(CustomerMatchingService.US_SALES))
                             .satisfies(testFun.apply(Arrays.asList("IVANOVA", "EUR")));
        // no  customers from others countries
        PAssert.that(customerSales.get(CustomerMatchingService.UNDEF_SALES))
                             .satisfies(testFun.apply(Collections.EMPTY_LIST));
        
        pipeline.run().waitUntilFinish();

    }

    // test data 
    private static final List<String> customers = Arrays.asList(new String[]{
        "ALEX, CHERNYSHEV, POL, KRAKOW, JANA KAZCHARY 3/35, +48 516 420 276",
        "OLGA, IVANOVA, US, NY, BRONKS 1-12/1, +111 00 001 999"
    });

    private static final List<CustomerInfo> customersExpected = Arrays.asList(
            CustomerInfo.of(Optional.absent(), "ALEX", "CHERNYSHEV", "POL",
                    "KRAKOW", "JANA KAZCHARY 3/35", "+48 516 420 276", null),
            CustomerInfo.of(Optional.absent(), "OLGA", "IVANOVA", "US",
                    "NY", "BRONKS 1-12/1", "+111 00 001 999", null));

    private static final List<String> countryCodes = Arrays.asList(new String[]{
        "POL, POLAND", "US, USA", "UK, United Kingdom"
    });

    private static final List<String> sales = Arrays.asList(new String[]{
        "CHERNYSHEV, +48516420276, STORE.1, PROD.1, TX-1000, 1, 10.20, 0.00, EUR",
        "IVANOVA, +11100001999, STORE.2, PROD.10, TX-4000, 1, 50.10, 0.00, EUR",
        "DONALN, +11111111111, STORE.4, PROD.100, TX-2233, 1, 30.40, 4.00, USD"
    });

    private static final List<String> stores = Arrays.asList(new String[]{
        "STORE.1,  APPLE",
        "STORE.2,  FURLA",
        "STORE.4,  HOME DEPOT",});

    private static final List<String> regionalDiscounts = Arrays.asList(new String[]{
        "EUR,  2",
        "USD,  5"
    });

}
