Dataflow small test 

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
