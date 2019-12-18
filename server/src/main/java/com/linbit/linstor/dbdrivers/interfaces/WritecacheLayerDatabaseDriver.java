package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.storage.data.adapter.writecache.WritecacheRscData;
import com.linbit.linstor.storage.data.adapter.writecache.WritecacheVlmData;

public interface WritecacheLayerDatabaseDriver
{
    ResourceLayerIdDatabaseDriver getIdDriver();

    void persist(WritecacheRscData<?> writecacheRscDataRef) throws DatabaseException;
    void delete(WritecacheRscData<?> writecacheRscDataRef) throws DatabaseException;

    void persist(WritecacheVlmData<?> writecacheVlmDataRef) throws DatabaseException;
    void delete(WritecacheVlmData<?> writecacheVlmDataRef) throws DatabaseException;
}
