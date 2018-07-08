package com.acme.dataflow.dofns;

import com.acme.dataflow.model.SaleTx;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;
import org.junit.Rule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class SalesKVParserDoFnTest {

    @Rule
    public TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testSalesKVParser() {

        SalesKVParserDoFn parser = new SalesKVParserDoFn();

        PCollection<KV<String, SaleTx>> result = pipeline.apply(Create.of(SALES)).apply(ParDo.of(parser));
    
        SerializableFunction<Iterable<KV<String, SaleTx>>, Void> sf = (Iterable<KV<String, SaleTx>> it) -> {
                    it.forEach((KV<String, SaleTx> kv) -> {
                        assertTrue(kv.getKey().equalsIgnoreCase(kv.getValue().normalizedPhoneNumber));
                            });
                    return null;
                };

        PAssert.that(result).containsInAnyOrder(Arrays.asList(
                KV.of("+48516420276", SaleTx.of("CHERNYSHEV", "+48516420276", "STORE.1", "PROD.1", "TX-1000", 1, 
                          BigDecimal.valueOf(10.21), BigDecimal.valueOf(0.0), "EUR")),
                  KV.of("+11100001999", SaleTx.of("IVANOVA", "+11100001999", "STORE.2", "PROD.10", "TX-4000", 1, 
                          BigDecimal.valueOf(50.00), BigDecimal.valueOf(0.0), "EUR")),
                KV.of("+32011012", SaleTx.of("DONALN", "+32011012", "STORE.4", "PROD.100", "TX-2233", 1,
                        BigDecimal.valueOf(30.40), BigDecimal.valueOf(4.50), "USD"))));
        
        pipeline.run().waitUntilFinish();
    }

    private static final List<String> SALES = Arrays.asList(new String[] {
        "CHERNYSHEV, +48516420276, STORE.1, PROD.1, TX-1000, 1, 10.21, 0.0, EUR", 
        "IVANOVA, +11100001999, STORE.2, PROD.10, TX-4000, 1, 50.00, 0.00, EUR", 
        "DONALN, +32011012, STORE.4, PROD.100, TX-2233, 1, 30.40, 4.50, USD"
    });
}
