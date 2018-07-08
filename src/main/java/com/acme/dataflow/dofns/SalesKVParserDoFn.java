package com.acme.dataflow.dofns;

import com.acme.dataflow.model.SaleTx;
import java.math.BigDecimal;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.repackaged.org.apache.commons.lang3.StringUtils;
import static org.apache.beam.sdk.repackaged.org.apache.commons.lang3.StringUtils.SPACE;
import org.apache.beam.sdk.values.KV;

/**
* 
*  csv line  => KV { pnoneNumber -> SaleTx }
*/

@Slf4j
public class SalesKVParserDoFn extends AbstractCSVKVEntityParser<SaleTx> {

    @ProcessElement
    @Override
    public void processElement(ProcessContext c) {
        String[] parts = pattern.split(c.element());
        if (parts.length < 9) {
            log.warn("bad record  " + c.element());
        } else {
            c.output(KV.of(parts[1], buildEntity(parts)));
        }
    }

    protected SaleTx buildEntity(String[] parts) {
        
        return SaleTx.of(
                parts[0], // lastCustomerName,
                normalize(parts[1]), // normalizedPhoneNumber,
                parts[2], // storeId,
                parts[3], //  productCode,
                parts[4], //  txId,
                Integer.parseInt(parts[5]), //  qnty
                BigDecimal.valueOf(Double.parseDouble(parts[6])), //  price
                BigDecimal.valueOf(Double.parseDouble(parts[7])), //  dicsount
                parts[8]);  //  currencyCode)
    }

    private String normalize(String part) {
        return StringUtils.remove(part, SPACE);
    }

}