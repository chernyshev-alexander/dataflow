package com.acme.dataflow.model;

import java.io.Serializable;
import java.math.BigDecimal;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@Data
@ToString(includeFieldNames = true)
@EqualsAndHashCode
@RequiredArgsConstructor(staticName = "of")
public final class SaleTx implements Serializable {

    public final String lastCustomerName;
    public final String normalizedPhoneNumber;
    public final String storeId;
    public final String productCode;
    public final String txId;
    public final Integer quantity;
    public final BigDecimal price;
    public final BigDecimal discount;
    public final String currencyCode;

}
