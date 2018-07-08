package com.acme.dataflow.dofns;

import java.util.regex.Pattern;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public abstract class AbstractCSVKVEntityParser<T> extends DoFn<String, KV<String, T>> {

    public static final String COMMA_SPLITTER_EXP_DEFAULT = "\\s*,\\s*";
    
    final Pattern pattern = Pattern.compile(COMMA_SPLITTER_EXP_DEFAULT);

    public abstract void processElement(ProcessContext c);

}
