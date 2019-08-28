package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeRscData;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.utils.Pair;

import java.util.Set;

public interface NvmeLayerDatabaseDriver
{
    ResourceLayerIdDatabaseDriver getIdDriver();

    // NvmeRscData methods
    void create(NvmeRscData drbdRscData) throws DatabaseException;
    void delete(NvmeRscData drbdRscData) throws DatabaseException;

    // NvmeVlmData methods
    void persist(NvmeVlmData drbdVlmData) throws DatabaseException;
    void delete(NvmeVlmData drbdVlmData) throws DatabaseException;

    // methods only used for loading
    Pair<? extends RscLayerObject, Set<RscLayerObject>> load(
        Resource rscRef,
        int idRef,
        String rscSuffixRef,
        RscLayerObject parentRef
    );
}
