package com.acme.dataflow.model;

import java.io.Serializable;
import java.math.BigDecimal;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@ToString(includeFieldNames = true)
@EqualsAndHashCode
@Builder
public class PosTransaction implements Serializable {

    private String magnetHolderName;
    private String txTimeStamp;
    private String storeId, terminalId;
    private BigDecimal amount;
}
