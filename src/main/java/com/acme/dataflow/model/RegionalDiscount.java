package com.acme.dataflow.model;

import java.io.Serializable;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@Data
@ToString(includeFieldNames = true)
@EqualsAndHashCode
@RequiredArgsConstructor(staticName = "of")
public final class RegionalDiscount implements Serializable {
    
    public final String currencyCode;
    public final Double persentOfDiscount;
    
}
