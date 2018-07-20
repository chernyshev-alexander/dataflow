package com.acme.dataflow;

import com.google.api.services.bigquery.model.TableRow;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;

import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Slf4j
@RunWith(JUnit4.class)
public class BQSimplePipeLineTest {

    @Rule
    public TestPipeline testPipeline = TestPipeline.create();

    BQSimplePipeLine bqSimplePipeLine;

    @Before
    public void before() {
        bqSimplePipeLine = new BQSimplePipeLine(testPipeline);
    }

    @Test
    public void testReadInputs() {

        PAssert.that(bqSimplePipeLine.readInputs()).satisfies((Iterable<Long> input) -> {
            Assert.assertThat(
                    StreamSupport.stream(input.spliterator(), false).limit(4).collect(Collectors.toList()),
                    CoreMatchers.hasItems(0L, 1L, 2L, 3L));
            return null;
        });

        bqSimplePipeLine.pipeline.run();
    }

    @Test
    public void testLongToTableRowDoFn() {

        PCollection<TableRow> result = bqSimplePipeLine.readInputs()
                .apply(bqSimplePipeLine.getLongToTableRowDoFn());

        PAssert.that(result).satisfies((Iterable<TableRow> it) -> {
            List<TableRow> ltr = StreamSupport.stream(it.spliterator(), false).limit(1).collect(Collectors.toList());
            Assert.assertTrue(ltr.size() == 1 && (int) ltr.get(0).get(BQSimplePipeLine.FIELD_NAME) == 1);
            return null;
        });

        bqSimplePipeLine.pipeline.run();
    }

    @Test
    public void testGlobalCombineDoFn() {

        Combine.Globally<TableRow, TableRow> combiner = bqSimplePipeLine.getGlobalCombiner();

        PCollection<TableRow> input = bqSimplePipeLine.readInputs()
                .apply(bqSimplePipeLine.getLongToTableRowDoFn());

        PCollection<TableRow> result = combiner.expand(input);

        PAssert.that(result).satisfies((Iterable<TableRow> it) -> {
            List<TableRow> ltr = StreamSupport.stream(it.spliterator(), false).limit(1).collect(Collectors.toList());
            Assert.assertTrue(ltr.size() == 1 && (int) ltr.get(0).get(BQSimplePipeLine.FIELD_NAME) == 1);
            return null;
        });

        bqSimplePipeLine.pipeline.run();
    }

    @Test
    public void testGlobalCombiner() {

        Combine.Globally<TableRow, TableRow> combiner = bqSimplePipeLine.getGlobalCombiner();

        PCollection<TableRow> ls = bqSimplePipeLine.pipeline.apply(Create.of(tableRows));

        PCollection<TableRow> result = ls.apply(combiner);

        PAssert.that(result).satisfies((Iterable<TableRow> it) -> {
            List<TableRow> ltr = StreamSupport.stream(it.spliterator(), false).limit(1).collect(Collectors.toList());
            Assert.assertTrue(ltr.size() == 1 && (int) ltr.get(0).get(BQSimplePipeLine.FIELD_NAME) == 1);
            return null;
        });

        bqSimplePipeLine.pipeline.run();
    }

    @Test
    public void testExecuteInternal() {

        PCollection<TableRow> ls = bqSimplePipeLine.executeInternal(bqSimplePipeLine.readInputs());

        PAssert.that(ls).satisfies((Iterable<TableRow> it) -> {
            List<TableRow> ltr = StreamSupport.stream(it.spliterator(), false).limit(1).collect(Collectors.toList());
            Assert.assertTrue(ltr.size() == 1 && (int) ltr.get(0).get(BQSimplePipeLine.FIELD_NAME) == 1);
            return null;
        });

        bqSimplePipeLine.pipeline.run();
    }

    static final List<TableRow> tableRows = Arrays.asList(new TableRow[]{
        new TableRow().set(BQSimplePipeLine.FIELD_NAME, 1L),
        new TableRow().set(BQSimplePipeLine.FIELD_NAME, 1L)
    });

}
