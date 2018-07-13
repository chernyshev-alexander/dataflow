package com.acme.dataflow.model;

import java.io.Serializable;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.experimental.Wither;
import org.apache.beam.sdk.repackaged.com.google.common.base.Optional;

@Data
@ToString(includeFieldNames = true)
@EqualsAndHashCode
@RequiredArgsConstructor(staticName = "of")
public class CustomerInfo implements Serializable {

    public final Optional<String> uniqueIdentity;
    
    public final String firstName, lastName;
    public final String countryCode, city;
    public final String lineAddress;
    public final String phoneNo;
    
    @Wither public final String countryName;

}
