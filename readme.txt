Dataflow small test 

pipeline steps

1. [C1: customers] join [countries] = [C2 : customers + country name]
2. [C2 : customers + country name] split to [C2-1: customers EU], [C2-2:customers USA], [C2-3:customers UNDEF]
3. todo

How to run :  maven test

28/06 - initial commit
29/06 - testEnrichCustomerWithCountryName() implemented , aka

 select customer{countryName = country.name}
        from customer
        left join country 
     on customer.countryCode = country.countryCode

06/07   1. implemented testBranchCustomersByCountryCode 
            (splitted main pipeline to 3 pipelines by country codes)
        2. refactored : all Dofn's moved to dofns package

06/07 - added discounts. sales, stores and corresponded DoFns parsers

13/07 - implemented customer sales (makeSalesReportByVendorsWithRegionalDiscount)
