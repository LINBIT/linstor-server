package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.storage.data.adapter.nvme.NvmeRscData;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeVlmData;

import java.sql.SQLException;

public interface NvmeLayerDatabaseDriver
{
    ResourceLayerIdDatabaseDriver getIdDriver();

    // NvmeRscData methods
    void create(NvmeRscData drbdRscData) throws SQLException;
    void delete(NvmeRscData drbdRscData) throws SQLException;

    // NvmeVlmData methods
    void persist(NvmeVlmData drbdVlmData) throws SQLException;
    void delete(NvmeVlmData drbdVlmData) throws SQLException;
}
