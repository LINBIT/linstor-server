package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.storage.data.adapter.luks.LuksRscData;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.utils.Pair;

import java.util.Set;

public interface LuksLayerDatabaseDriver
{
    ResourceLayerIdDatabaseDriver getIdDriver();

    void persist(LuksRscData luksRscDataRef) throws DatabaseException;
    void delete(LuksRscData luksRscDataRef) throws DatabaseException;

    void persist(LuksVlmData luksVlmDataRef) throws DatabaseException;
    void delete(LuksVlmData luksVlmDataRef) throws DatabaseException;

    SingleColumnDatabaseDriver<LuksVlmData, byte[]> getVlmEncryptedPasswordDriver();

    // methods only used for loading
    Pair<? extends RscLayerObject, Set<RscLayerObject>> load(
        Resource rscRef,
        int idRef,
        String rscSuffixRef,
        RscLayerObject parentRef
    )
        throws DatabaseException;
}
