package com.acme.dataflow;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

public class BQSimplePipeLine {

    public final Pipeline pipeline;

    static final String FIELD_NAME = "value";

    public BQSimplePipeLine(Pipeline pipeline) {
        this.pipeline = pipeline;
    }

    static class LongToTableRowDoFn extends DoFn<Long, TableRow> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            TableRow row = new TableRow().set(FIELD_NAME, 1L);
            c.outputWithTimestamp(row, c.timestamp());
        }
    }
    
    private GenerateSequence getInputSeq() {
        return GenerateSequence.from(0).to(1000L)
                .withMaxReadTime(Duration.standardSeconds(20))
                .withRate(4, Duration.standardSeconds(1));
    }

    private Window<TableRow> getFixedWindows(Duration duration) {
        return Window.<TableRow>into(FixedWindows.of(duration));
    }

    protected ParDo.SingleOutput<Long, TableRow> getLongToTableRowDoFn() {
        return ParDo.of(new LongToTableRowDoFn());
    }

    protected Combine.Globally<TableRow, TableRow> getGlobalCombiner() {
        return Combine.globally((Iterable<TableRow> it) -> {
            long cnt = StreamSupport.stream(it.spliterator(), false).map((TableRow tr) -> {
                final Object obj = tr.get(FIELD_NAME);
                // strange but can be Integer and Long !!!  
                return (obj instanceof Integer) ? ((Integer) obj).longValue() : (Long) obj;
            }).count();

            return new TableRow().set(FIELD_NAME, cnt);
            
        }).withoutDefaults();
    }

    private BigQueryIO.Write<TableRow> getBqTable(String tableName) {

        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName(FIELD_NAME).setType("LONG"));
        TableSchema schema = new TableSchema().setFields(fields);

        return BigQueryIO.writeTableRows().to(tableName).withSchema(schema)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE);
    }

    public PCollection<Long> readInputs() {
        return pipeline.apply(getInputSeq());
    }

    public void writeOutputs(PCollection<TableRow> ls) {
        ls.apply(getBqTable("project_id:dataset_id.table_id"));
    }
    
    // Main workflow
    protected PCollection<TableRow> executeInternal(PCollection<Long> inputs) {
        return inputs
                .apply(getLongToTableRowDoFn())
                .apply(getFixedWindows(Duration.standardSeconds(1)))
                .apply(getGlobalCombiner());
    }
    
    public void execute() {
        writeOutputs(executeInternal(readInputs()));
    }

    public interface BQSimpleOptions extends PipelineOptions {

        @Description("Path of the file to read from")
        @Default.String("gs://huse-holding/input.txt")
        String getInputFile();

        void setInputFile(String value);

        @Description("Path of the file to write to")
        @Default.InstanceFactory(OutputFactory.class)
        String getOutput();

        void setOutput(String value);

        // returns "gs://${YOUR_STAGING_DIRECTORY}/output.txt" as the default destination.
        class OutputFactory implements DefaultValueFactory<String> {

            @Override
            public String create(PipelineOptions options) {
                PipelineOptions opt = options.as(PipelineOptions.class);
                if (opt.getTempLocation() != null) {
                    return GcsPath.fromUri(opt.getTempLocation()).resolve("output.txt").toString();
                } else {
                    throw new IllegalArgumentException("Must specify --output or --stagingLocation");
                }
            }
        }

        public static void main(String[] args) {

            BQSimpleOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                    .as(BQSimpleOptions.class);

            Pipeline p = Pipeline.create(options);
            
            new BQSimplePipeLine(Pipeline.create(options)).execute();
            
            p.run().waitUntilFinish();
        }
    }
}
