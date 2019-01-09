package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.storage.interfaces.layers.cryptsetup.CryptSetupVlmObject;

public interface CryptSetupDatabaseDriver
{
    SingleColumnDatabaseDriver<CryptSetupVlmObject, char[]> getPasswordDriver();

}
