package com.acme.dataflow;

import com.acme.dataflow.model.CustomerInfo;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.repackaged.com.google.common.base.Optional;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Test;

public class CustomerMatchingServiceTest {

    //@Rule
    //public TemporaryFolder tf = new TemporaryFolder();
    CustomerMatchingService.Options options;  // pipeline options from command line
    CustomerMatchingService service;          // our pipeline as service

    @Before
    public void before() {
        options = TestPipeline.testingPipelineOptions().as(CustomerMatchingService.Options.class);
        service = new CustomerMatchingService();
    }

    // Case 1. Individual 
    //
    // Testing individual DoFn function, one step of the pipeline
    //
    // DoFnTester is deprecated and google gays recommended to use Pipeline with DirectRunner
    //
    
    @Test
    public void testCustomerParser() throws Exception {
        
        Pipeline pipeline = Pipeline.create(options);
        PCollection<String> testInput = pipeline.apply(Create.of(customerLines)).setCoder(StringUtf8Coder.of());
        PCollection<CustomerInfo> customers = testInput.apply(ParDo.of(service.getCustomerParser()));
        
        PAssert.that(customers).containsInAnyOrder(expected);
        
        pipeline.run().waitUntilFinish(); // execute pipeline
        
    }

    // Case 2  Full pipeline
    //
    // End-to-end testing of the pipeline
    // 
    
    @Test
    public void testMatchingCustomer() {

        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> testInput = pipeline.apply(Create.of(customerLines)).setCoder(StringUtf8Coder.of());
        PCollection<CustomerInfo> customers = testInput.apply(service.getMainTransformer());

        // add distributed assert to the data execution plan
        PAssert.that(customers).containsInAnyOrder(expected);
        // and run pipeline
        pipeline.run().waitUntilFinish(Duration.standardSeconds(10));

    }
    
    private static final List<String> customerLines = Arrays.asList(new String[]{
        "ALEX, CHERNYSHEV, POLAND, KRAKOW, JANA KAZCHARY 3/35, +48 516 420 276",
        "OLGA, IVANOVA, USA, NY, BRONKS 1-12/1, +111 00 001 999"
    });

    private static final List<CustomerInfo> expected = Arrays.asList(
            CustomerInfo.of(Optional.absent(), "ALEX", "CHERNYSHEV", "POLAND",
                    "KRAKOW", "JANA KAZCHARY 3/35", "+48 516 420 276"),
            CustomerInfo.of(Optional.absent(), "OLGA", "IVANOVA", "USA",
                    "NY", "BRONKS 1-12/1", "+111 00 001 999"));
    
    
}
