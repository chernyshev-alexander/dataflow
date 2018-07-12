package com.acme.dataflow.model;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@Data
@ToString(includeFieldNames = true)
@EqualsAndHashCode
@RequiredArgsConstructor(staticName = "of")
public final class CustomerSales implements Serializable {
    
    public final CustomerInfo customer;
       
    // fix it
    public final List<SaleTx> sales = Collections.EMPTY_LIST;
}
