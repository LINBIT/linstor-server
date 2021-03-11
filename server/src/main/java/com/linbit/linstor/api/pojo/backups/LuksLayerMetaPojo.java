package com.linbit.linstor.api.pojo.backups;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class LuksLayerMetaPojo
{
    private final String masterPassword;
    private final String masterCryptHash;
    private final String masterCryptSalt;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public LuksLayerMetaPojo(
        @JsonProperty("masterPassword") String masterPasswordRef,
        @JsonProperty("masterCryptHash") String masterCryptHashRef,
        @JsonProperty("masterCryptSalt") String masterCryptSaltRef
    )
    {
        masterPassword = masterPasswordRef;
        masterCryptHash = masterCryptHashRef;
        masterCryptSalt = masterCryptSaltRef;
    }

    public String getMasterPassword()
    {
        return masterPassword;
    }

    public String getMasterCryptHash()
    {
        return masterCryptHash;
    }

    public String getMasterCryptSalt()
    {
        return masterCryptSalt;
    }
}
