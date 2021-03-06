package com.acme.dataflow.dofns;

import com.acme.dataflow.model.RegionalDiscount;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;
import org.junit.Rule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PersentOfDiscountByRegionKVParserDoFnTest {

    @Rule
    public TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testPersentOfDiscountByRegionKVParser() {

        RegionalDiscountKVParserDoFn parser = new RegionalDiscountKVParserDoFn();

        PCollection<KV<String, RegionalDiscount>> result = pipeline.apply(Create.of(PERSENT_OF_DISCOUNT_BY_CURRENCY))
                .apply(ParDo.of(parser));

        PAssert.that(result).containsInAnyOrder(Arrays.asList(KV.of("EUR", RegionalDiscount.of("EUR", 2.0)),
                                KV.of("USD", RegionalDiscount.of("USD", 5.0))));

        pipeline.run().waitUntilFinish();
    }

    private static final List<String> PERSENT_OF_DISCOUNT_BY_CURRENCY = Arrays.asList(new String[]{
        "EUR,  2",
        "USD,  5"
    });

}
