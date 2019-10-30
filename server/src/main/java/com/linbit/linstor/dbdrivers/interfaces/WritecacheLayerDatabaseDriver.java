package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.storage.data.adapter.writecache.WritecacheRscData;
import com.linbit.linstor.storage.data.adapter.writecache.WritecacheVlmData;

public interface WritecacheLayerDatabaseDriver
{
    ResourceLayerIdDatabaseDriver getIdDriver();

    void persist(WritecacheRscData luksRscDataRef) throws DatabaseException;
    void delete(WritecacheRscData luksRscDataRef) throws DatabaseException;

    void persist(WritecacheVlmData luksVlmDataRef) throws DatabaseException;
    void delete(WritecacheVlmData luksVlmDataRef) throws DatabaseException;
}
