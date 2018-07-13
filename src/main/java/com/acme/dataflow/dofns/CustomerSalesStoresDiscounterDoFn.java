package com.acme.dataflow.dofns;

import avro.shaded.com.google.common.base.Optional;
import com.acme.dataflow.model.CustomerSales;
import com.acme.dataflow.model.RegionalDiscount;
import com.acme.dataflow.model.Store;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;

@Slf4j
public class CustomerSalesStoresDiscounterDoFn extends DoFn<CustomerSales, CustomerSales> {

    final Counter handledCounter = Metrics.counter(CustomerSalesStoresDiscounterDoFn.class, "handled");

    private final PCollectionView<Map<String, Store>> storesView;
    private final PCollectionView<Map<String, RegionalDiscount>> discountsView;

    private Map<String, Store> storesMap;
    private Map<String, RegionalDiscount> discountsMap;

    public CustomerSalesStoresDiscounterDoFn(
            PCollectionView<Map<String, Store>> storesView,
            PCollectionView<Map<String, RegionalDiscount>> discountsView) {

        this.storesView = storesView;
        this.discountsView = discountsView;
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) {

        // create a new instance for modifications
        CustomerSales customerSales = ctx.element();
        CustomerSales result = CustomerSales.of(customerSales.getCustomer());

        result.getSales().addAll(customerSales.getSales());

        // pass through the all sales in pcollection 
        // and make aggregated map view { storeName -> max applied discount for the customer }
        //
        ctx.element().getSales().forEach(stx -> {

            Optional<RegionalDiscount> discountOpt = Optional.fromNullable(
                    ctx.sideInput(discountsView).getOrDefault(stx.currencyCode, null));
            Optional<Store> storeOpt = Optional.fromNullable(
                    ctx.sideInput(storesView).getOrDefault(stx.storeId, null));

            if (discountOpt.isPresent() && storeOpt.isPresent()) {
                String storeName = storeOpt.get().storeName;
                Double discountPersent = discountOpt.get().persentOfDiscount;

                result.getMaxDiscountsPerStoreName().compute(storeName,
                        (k, v) -> (v == null || v < discountPersent) ? discountPersent : v);

            } else {
                log.warn("skipped : integrity data violation {}", stx);
            }
        });

        log.warn("{}", result);
        ctx.output(result);

        handledCounter.inc();
    }
}
