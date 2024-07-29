package com.linbit.linstor.layer;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.AbsRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

import java.util.Map;

public class AbsLayerHelperUtils
{
    public static final String RENAME_STOR_POOL_DFLT_KEY = "*";

    private AbsLayerHelperUtils()
    {
    }

    public static <RSC extends AbsResource<RSC>> @Nullable StorPool getStorPool(
        AccessContext apiCtx,
        AbsVolume<RSC> absVlmRef,
        AbsRscData<RSC, ? extends VlmProviderObject<RSC>> rscLayerData,
        @Nullable StorPool storPool,
        Map<String, String> renameStorPoolMap,
        @Nullable ApiCallRc apiCallRc
    ) throws AccessDeniedException, InvalidNameException
    {
        return storPool == null ?
            null :
            getStorPool(
                apiCtx,
                absVlmRef,
                rscLayerData,
                storPool.getName().displayValue,
                renameStorPoolMap,
                apiCallRc
            );
    }

    public static <RSC extends AbsResource<RSC>> StorPool getStorPool(
        AccessContext apiCtx,
        AbsVolume<RSC> absVlmRef,
        AbsRscData<RSC, ? extends VlmProviderObject<RSC>> rscLayerData,
        String storPoolName,
        Map<String, String> renameStorPoolMap,
        @Nullable ApiCallRc apiCallRc
    ) throws AccessDeniedException, InvalidNameException
    {
        StorPool storPool = null;
        String renamedStorPool;
        VlmProviderObject<RSC> vlmProviderObject = rscLayerData.getVlmProviderObject(absVlmRef.getVolumeNumber());
        // if we are in the process of creating vlmProviderObject, it will be null here - which means exists is
        // false even if we can't check it
        if (vlmProviderObject == null || !vlmProviderObject.exists())
        {
            renamedStorPool = renameStorPoolMap.get(storPoolName);
            if (renamedStorPool == null)
            {
                // if no mapping is found, first check if there is a dflt-entry
                renamedStorPool = renameStorPoolMap.get(RENAME_STOR_POOL_DFLT_KEY);
                if (renamedStorPool == null)
                {
                    // if no mapping is found, it is assumed that the old storPoolName still applies
                    renamedStorPool = storPoolName;
                }
            }
        }
        else
        {
            // vlmLayerData exists, which means we aren't allowed to switch storpool
            renamedStorPool = storPoolName;
            if (apiCallRc != null)
            {
                apiCallRc.add(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.WARN_STORPOOL_RENAME_NOT_ALLOWED,
                        "Renaming the storpool is not possible because the layer-data already exists"
                    )
                );
            }
        }

        Node node = absVlmRef.getAbsResource().getNode();
        storPool = node.getStorPool(apiCtx, new StorPoolName(renamedStorPool));
        if (storPool == null)
        {
            if (!storPoolName.equals(renamedStorPool))
            {
                storPool = node.getStorPool(apiCtx, new StorPoolName(storPoolName));
            }
            if (storPool == null)
            {
                throw new ImplementationError(
                    "StorPool not found: " + node + " " + renamedStorPool
                );
            }
        }
        return storPool;
    }
}
