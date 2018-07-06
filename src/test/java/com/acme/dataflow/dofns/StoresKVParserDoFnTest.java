package com.acme.dataflow.dofns;

import com.acme.dataflow.DataFlowConfig;
import com.acme.dataflow.model.Store;
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
public class StoresKVParserDoFnTest {
    
    @Rule
    public TestPipeline pipeline = TestPipeline.create();
    
    @Test
    public void testStoresKVParser() {

            StoresKVParserDoFn parser = new StoresKVParserDoFn();

        PCollection<KV<String, Store>> result = pipeline.apply(Create.of(STORES)).apply(ParDo.of(parser));

        PAssert.that(result).containsInAnyOrder(Arrays.asList(
                KV.of("STORE.1", Store.of("STORE.1", "APPLE")),
                KV.of("STORE.4", Store.of("STORE.4", "HOME DEPOT")),
                KV.of("STORE.2", Store.of("STORE.2", "FURLA"))));


        pipeline.run().waitUntilFinish();
    }

   private static final List<String> STORES = Arrays.asList(new String[] {
       "STORE.1,  APPLE", 
       "STORE.2,  FURLA", 
       "STORE.4,  HOME DEPOT", 
    }); 
}