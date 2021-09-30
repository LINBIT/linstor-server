package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.utils.Pair;

import java.util.Set;

public interface LuksLayerCtrlDatabaseDriver extends LuksLayerDatabaseDriver
{
    <RSC extends AbsResource<RSC>> Pair<? extends AbsRscLayerObject<RSC>, Set<AbsRscLayerObject<RSC>>> load(
        RSC rscRef,
        int idRef,
        String rscSuffixRef,
        AbsRscLayerObject<RSC> parentRef
    )
        throws DatabaseException;

    /*
     * Implement these methods to increase loading performance by for example fetching all data with one single request
     * instead of creating new requests for each object.
     *
     * If the underlying DBEngine is performant enough (like SQL), there is no need to implement these methods
     */
    default void fetchForLoadAll()
    {
    }

    default void clearLoadAllCache()
    {
    }

    default String getId(LuksVlmData<?> luksVlmDataRef)
    {
        return "(LayerRscId=" + luksVlmDataRef.getRscLayerId() +
            ", VlmNr=" + luksVlmDataRef.getVlmNr().value +
            ")";
    }
}
