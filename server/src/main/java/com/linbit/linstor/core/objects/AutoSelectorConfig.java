package com.linbit.linstor.core.objects;

import com.linbit.linstor.DbgInstanceUuid;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ProtectedObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionObject;

import java.util.List;

public interface AutoSelectorConfig
    extends TransactionObject, DbgInstanceUuid, ProtectedObject
{
    ResourceGroup getResourceGroup();

    Integer getReplicaCount(AccessContext accCtxRef)
        throws AccessDeniedException;

    String getStorPoolNameStr(AccessContext accCtxRef)
        throws AccessDeniedException;

    List<String> getDoNotPlaceWithRscList(AccessContext accCtxRef)
        throws AccessDeniedException;

    String getDoNotPlaceWithRscRegex(AccessContext accCtxRef)
        throws AccessDeniedException;

    List<String> getReplicasOnSameList(AccessContext accCtxRef)
        throws AccessDeniedException;

    List<String> getReplicasOnDifferentList(AccessContext accCtxRef)
        throws AccessDeniedException;

    List<DeviceLayerKind> getLayerStackList(AccessContext accCtxRef)
        throws AccessDeniedException;

    List<DeviceProviderKind> getProviderList(AccessContext accCtxRef)
        throws AccessDeniedException;

    Boolean getDisklessOnRemaining(AccessContext accCtxRef)
        throws AccessDeniedException;

    AutoSelectFilterApi getApiData(AccessContext accCtx)
        throws AccessDeniedException;
}
