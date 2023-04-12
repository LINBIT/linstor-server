package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeRscData;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeVlmData;

public interface NvmeLayerDatabaseDriver
{
    LayerResourceIdDatabaseDriver getIdDriver();

    // NvmeRscData methods
    void create(NvmeRscData<?> drbdRscData) throws DatabaseException;
    void delete(NvmeRscData<?> drbdRscData) throws DatabaseException;

    // NvmeVlmData methods
    void persist(NvmeVlmData<?> drbdVlmData) throws DatabaseException;
    void delete(NvmeVlmData<?> drbdVlmData) throws DatabaseException;
}
