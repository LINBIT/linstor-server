package com.linbit.linstor.storage.layer.adapter.cryptsetup;

import com.linbit.linstor.storage.layer.data.CryptSetupData;
import com.linbit.linstor.storage.layer.data.State;

import java.util.Collections;
import java.util.List;

public class CryptSetupStltData implements CryptSetupData
{
    byte[] password;
    boolean failed;
    String identifier;
    boolean opened;

    // TODO maybe introduce "OPEN", "CLOSED", "UNINITIALIZED" or something...
    List<State> states = Collections.emptyList();

    public CryptSetupStltData(byte[] passwordRef, String identifierRef)
    {
        password = passwordRef;
        identifier = identifierRef;
    }

    @Override
    public byte[] getPassword()
    {
        return password;
    }

    @Override
    public boolean isFailed()
    {
        return failed;
    }

    @Override
    public boolean exists()
    {
        return opened;
    }

    @Override
    public List<? extends State> getStates()
    {
        return states;
    }
}
