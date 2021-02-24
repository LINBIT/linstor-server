package com.linbit.linstor.api.pojo.backups;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class LuksLayerMetaPojo
{
    private final String masterPassword;
    private final String masterCryptHash;
    private final String masterCryptSalt;
    private final Map<Integer, String> volumePasswords;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public LuksLayerMetaPojo(
        @JsonProperty("masterPassword") String masterPasswordRef,
        @JsonProperty("masterCryptHash") String masterCryptHashRef,
        @JsonProperty("masterCryptSalt") String masterCryptSaltRef,
        @JsonProperty("volumePasswords") Map<Integer, String> volumePasswordsRef
    )
    {
        masterPassword = masterPasswordRef;
        masterCryptHash = masterCryptHashRef;
        masterCryptSalt = masterCryptSaltRef;
        volumePasswords = volumePasswordsRef;
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

    public Map<Integer, String> getVolumePasswords()
    {
        return volumePasswords;
    }
}
