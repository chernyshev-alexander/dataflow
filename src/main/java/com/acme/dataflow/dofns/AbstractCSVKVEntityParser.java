package com.acme.dataflow.dofns;

import com.acme.dataflow.DataFlowConfig;
import java.util.regex.Pattern;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public abstract class AbstractCSVKVEntityParser<T> extends DoFn<String, KV<String, T>> {

    final Pattern pattern = Pattern.compile(DataFlowConfig.COMMA_SPLITTER_EXP_DEFAULT);

    public abstract void processElement(ProcessContext c);

}
