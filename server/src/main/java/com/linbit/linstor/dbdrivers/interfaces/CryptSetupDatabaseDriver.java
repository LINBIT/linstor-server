package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.storage.layer.data.CryptSetupData;

public interface CryptSetupDatabaseDriver
{
    SingleColumnDatabaseDriver<CryptSetupData, char[]> getPasswordDriver();

}
