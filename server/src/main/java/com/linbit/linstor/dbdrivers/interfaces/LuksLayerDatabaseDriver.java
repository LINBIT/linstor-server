package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.luks.LuksRscData;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;

public interface LuksLayerDatabaseDriver
{
    LayerResourceIdDatabaseDriver getIdDriver();

    void persist(LuksRscData<?> luksRscDataRef) throws DatabaseException;

    void delete(LuksRscData<?> luksRscDataRef) throws DatabaseException;

    void persist(LuksVlmData<?> luksVlmDataRef) throws DatabaseException;

    void delete(LuksVlmData<?> luksVlmDataRef) throws DatabaseException;

    SingleColumnDatabaseDriver<LuksVlmData<?>, byte[]> getVlmEncryptedPasswordDriver();
}
