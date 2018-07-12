package com.acme.dataflow.dofns;

import java.util.regex.Pattern;

public interface CSVCommons {
    
    public static final String COMMA_SPLITTER_EXP_DEFAULT = "\\s*,\\s*";
    
    final Pattern pattern = Pattern.compile(COMMA_SPLITTER_EXP_DEFAULT);
  
    
}
