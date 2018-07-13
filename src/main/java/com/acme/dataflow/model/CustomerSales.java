package com.acme.dataflow.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
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
       
    public final List<SaleTx> sales = new ArrayList<>();
    
    public final Map<String, Double> maxDiscountsPerStoreName = new ConcurrentSkipListMap<>();
    
}
