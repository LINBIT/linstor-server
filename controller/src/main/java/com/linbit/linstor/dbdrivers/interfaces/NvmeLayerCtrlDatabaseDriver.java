package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.utils.Pair;

import java.util.Set;

public interface NvmeLayerCtrlDatabaseDriver extends NvmeLayerDatabaseDriver
{
    Pair<? extends RscLayerObject, Set<RscLayerObject>> load(
        Resource rscRef,
        int idRef,
        String rscSuffixRef,
        RscLayerObject parentRef
    );
}
