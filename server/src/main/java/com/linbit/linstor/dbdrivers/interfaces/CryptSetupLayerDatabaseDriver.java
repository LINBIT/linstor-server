package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.cryptsetup.CryptSetupRscData;
import com.linbit.linstor.storage.data.adapter.cryptsetup.CryptSetupVlmData;
import java.sql.SQLException;

public interface CryptSetupLayerDatabaseDriver
{
    ResourceLayerIdDatabaseDriver getIdDriver();

    void persist(CryptSetupRscData cryptRscDataRef) throws SQLException;
    void delete(CryptSetupRscData cryptRscDataRef) throws SQLException;

    void persist(CryptSetupVlmData cryptVlmDataRef) throws SQLException;
    void delete(CryptSetupVlmData cryptVlmDataRef) throws SQLException;

    SingleColumnDatabaseDriver<CryptSetupVlmData, byte[]> getVlmEncryptedPasswordDriver();
}
