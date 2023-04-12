package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.storage.data.adapter.cache.CacheRscData;
import com.linbit.linstor.storage.data.adapter.cache.CacheVlmData;

public interface CacheLayerDatabaseDriver
{
    LayerResourceIdDatabaseDriver getIdDriver();

    void persist(CacheRscData<?> cacheRscDataRef) throws DatabaseException;
    void delete(CacheRscData<?> cacheRscDataRef) throws DatabaseException;

    void persist(CacheVlmData<?> cacheVlmDataRef) throws DatabaseException;
    void delete(CacheVlmData<?> cacheVlmDataRef) throws DatabaseException;
}
