package com.linbit.linstor.storage.layer.adapter.cryptsetup;

import com.linbit.linstor.storage2.layer.data.CryptSetupData;

public class CryptSetupStltData implements CryptSetupData
{
    char[] password;
    boolean failed;

    public CryptSetupStltData(char[] passwordRef)
    {
        password = passwordRef;
    }

    @Override
    public char[] getPassword()
    {
        return password;
    }

    @Override
    public boolean isFailed()
    {
        return failed;
    }
}
