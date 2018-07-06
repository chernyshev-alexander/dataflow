package com.acme.dataflow;

import java.util.Arrays;
import java.util.List;
import com.acme.dataflow.dofns.SplitCustomerByCountryDoFn;
import com.acme.dataflow.model.CustomerInfo;
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
    public void testCustomerParser() {

        CustomerMatchingService service = new CustomerMatchingService(pipeline, options);
        PCollection<CustomerInfo> customers = service.readCustomers(Create.of(customerLines));

        PAssert.that(customers).containsInAnyOrder(customersExpected);
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testEnrichCustomerWithCountryName() {

        CustomerMatchingService service = new CustomerMatchingService(pipeline, options);

        PCollection<CustomerInfo> customers = service.readCustomers(Create.of(customerLines));
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
    public void testBranchCustomersByCountryCode() {

        CustomerMatchingService service = new CustomerMatchingService(pipeline, options);

        PCollection<CustomerInfo> customers = service.readCustomers(Create.of(customerLines));

        PCollectionTuple result = service.branchCustomersByCountryCode(customers);
        
        PCollection<CustomerInfo> euCustomers = result.get(SplitCustomerByCountryDoFn.TAG_EU_CUSTOMER);
        PCollection<CustomerInfo> usCustomers = result.get(SplitCustomerByCountryDoFn.TAG_USA_CUSTOMER);
        PCollection<CustomerInfo> undefCustomers = result.get(SplitCustomerByCountryDoFn.TAG_UDEF_COUNTRY_CUSTOMER);
        
        //  high-order help function 
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
    public void testMatchingCustomer() {
        //TODO

    }

    // test data 
    private static final List<String> customerLines = Arrays.asList(new String[]{
        "ALEX, CHERNYSHEV, POL, KRAKOW, JANA KAZCHARY 3/35, +48 516 420 276",
        "OLGA, IVANOVA, US, NY, BRONKS 1-12/1, +111 00 001 999"
    });

    private static final List<CustomerInfo> customersExpected = Arrays.asList(
            CustomerInfo.of(Optional.absent(), "ALEX", "CHERNYSHEV", "POL",
                    "KRAKOW", "JANA KAZCHARY 3/35", "+48 516 420 276", null),
            CustomerInfo.of(Optional.absent(), "OLGA", "IVANOVA", "US",
                    "NY", "BRONKS 1-12/1", "+111 00 001 999", null));

    private static final List<String> countryCodes = Arrays.asList(new String[]{
        "POL,  POLAND", "US, USA", "UK, United Kingdom"
    });

    private static final List<String> SALES = Arrays.asList(new String[] {
       "CHERNYSHEV, +48516420276, STORE.1, PROD.1, TX-1000, 1, 10.20 0.00 EUR", 
       "IVANOVA, +11100001999, STORE.2, PROD.10, TX-4000, 1, 50.10 0.00 EUR", 
       "DONALN, +11111111111, STORE.4, PROD.100, TX-2233, 1, 30.40 4.00 USD", 
    });
    
   private static final List<String> STORES = Arrays.asList(new String[] {
       "STORE.1,  APPLE", 
       "STORE.2,  FURLA", 
       "STORE.4,  HOME DEPOT", 
    });   

   private static final List<String> PERSENT_OF_DISCOUNT_BY_CURRENCY = Arrays.asList(new String[] {
       "EUR,  2", 
       "USD,  5"
    });   
   
}
