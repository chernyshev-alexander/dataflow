package com.acme.dataflow.dofns;

import lombok.extern.slf4j.Slf4j;
import com.acme.dataflow.model.CustomerInfo;
import org.apache.beam.sdk.repackaged.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

/**
 * input - CustomerInfo
 * output - CustomerInfo(POL, UK), CustomerInfo(US), CustomerInfo(<others>)
 */
@Slf4j
public class SplitCustomerByCountryDoFn extends DoFn<CustomerInfo, CustomerInfo> {
    
        public static final TupleTag<CustomerInfo> TAG_EU_CUSTOMER = new TupleTag<CustomerInfo>() {
        };
        public static final TupleTag<CustomerInfo> TAG_USA_CUSTOMER = new TupleTag<CustomerInfo>() {
        };
        public static final TupleTag<CustomerInfo> TAG_UDEF_COUNTRY_CUSTOMER = new TupleTag<CustomerInfo>() {
        };

        @ProcessElement
        public void processElement(ProcessContext c) {
            CustomerInfo customerInfo = c.element();
            
            if (StringUtils.isBlank(customerInfo.countryCode)) {
                log.info("skipped : countryCode is null or empty", customerInfo);
                return;
            }

            switch (customerInfo.countryCode) {
                case "POL":
                case "UK":
                    c.output(TAG_EU_CUSTOMER, customerInfo);
                    break;
                case "US":
                    c.output(TAG_USA_CUSTOMER, customerInfo);
                    break;
                default:
                    c.output(TAG_UDEF_COUNTRY_CUSTOMER, customerInfo);
            }
        }
    }
