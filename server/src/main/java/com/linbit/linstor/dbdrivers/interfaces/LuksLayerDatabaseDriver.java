package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.luks.LuksRscData;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;

import java.sql.SQLException;

public interface LuksLayerDatabaseDriver
{
    ResourceLayerIdDatabaseDriver getIdDriver();

    void persist(LuksRscData luksRscDataRef) throws SQLException;
    void delete(LuksRscData luksRscDataRef) throws SQLException;

    void persist(LuksVlmData luksVlmDataRef) throws SQLException;
    void delete(LuksVlmData luksVlmDataRef) throws SQLException;

    SingleColumnDatabaseDriver<LuksVlmData, byte[]> getVlmEncryptedPasswordDriver();
}
