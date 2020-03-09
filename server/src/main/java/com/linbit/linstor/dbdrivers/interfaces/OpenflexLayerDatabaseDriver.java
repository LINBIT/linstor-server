package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.nvme.OpenflexRscData;
import com.linbit.linstor.storage.data.adapter.nvme.OpenflexRscDfnData;
import com.linbit.linstor.storage.data.adapter.nvme.OpenflexVlmData;

public interface OpenflexLayerDatabaseDriver
{
    ResourceLayerIdDatabaseDriver getIdDriver();

    // OpenflexRscDfnData methos
    void create(OpenflexRscDfnData<?> ofRscDfnDataRef) throws DatabaseException;
    void delete(OpenflexRscDfnData<?> ofRscDfnDataRef) throws DatabaseException;
    SingleColumnDatabaseDriver<OpenflexRscDfnData<?>, String> getNqnDriver() throws DatabaseException;

    // OpenflexRscData methods
    void create(OpenflexRscData<?> ofRscData) throws DatabaseException;
    void delete(OpenflexRscData<?> ofRscData) throws DatabaseException;

    // OpenflexVlmData methods
    void persist(OpenflexVlmData<?> ofVlmData) throws DatabaseException;
    void delete(OpenflexVlmData<?> ofVlmData) throws DatabaseException;
}
