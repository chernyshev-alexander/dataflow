package com.acme.dataflow.dofns;

import com.acme.dataflow.DataFlowConfig;
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
public class CountryCodesParserTest {

    @Rule
    public TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testCountryCodeParser() {

        CountryCodesParser parser = new CountryCodesParser(DataFlowConfig.COMMA_SPLITTER_EXP_DEFAULT);

        PCollection<KV<String, String>> pCountryCodes = pipeline.apply(Create.of(COUNTRY_CODES))
                .apply(ParDo.of(parser));

        PAssert.that(pCountryCodes).containsInAnyOrder(KV.of("POL", "POLAND"), KV.of("US", "USA"),
                KV.of("UK", "United Kingdom"));

        pipeline.run().waitUntilFinish();
    }

    private static final List<String> COUNTRY_CODES = Arrays.asList(new String[]{
        "POL,  POLAND", "US, USA", "UK, United Kingdom"
    });
}
