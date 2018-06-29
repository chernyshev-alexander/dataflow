Dataflow small test 

How to run :  maven test

28/06 - initial commit
29/06 - testEnrichCustomerWithCountryName() implemented , aka

 select customer{countryName = country.name}
        from customer
        left join country 
        on customer.countryCode = country.countryCode

