package com.linbit.linstor.api.pojo.backups;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class LuksLayerMetaPojo
{
    private final String masterPassword;
    private final String masterCryptHash;
    private final String masterCryptSalt;
    private final Map<Integer, String> volumePasswords;

    public LuksLayerMetaPojo(
        String masterPasswordRef,
        String masterCryptHashRef,
        String masterCryptSaltRef,
        Map<Integer, String> volumePasswordsRef
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
