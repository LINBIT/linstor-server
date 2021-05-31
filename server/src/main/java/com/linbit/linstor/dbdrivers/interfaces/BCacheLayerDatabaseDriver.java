package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.bcache.BCacheRscData;
import com.linbit.linstor.storage.data.adapter.bcache.BCacheVlmData;

import java.util.UUID;

public interface BCacheLayerDatabaseDriver
{
    ResourceLayerIdDatabaseDriver getIdDriver();

    void persist(BCacheRscData<?> bcacheRscDataRef) throws DatabaseException;
    void delete(BCacheRscData<?> bcacheRscDataRef) throws DatabaseException;

    void persist(BCacheVlmData<?> bcacheVlmDataRef) throws DatabaseException;
    void delete(BCacheVlmData<?> bcacheVlmDataRef) throws DatabaseException;
    SingleColumnDatabaseDriver<BCacheVlmData<?>, UUID> getDeviceUuidDriver();
}
