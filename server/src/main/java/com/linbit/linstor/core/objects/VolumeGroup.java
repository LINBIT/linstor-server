package com.linbit.linstor.core.objects;

import com.linbit.linstor.DbgInstanceUuid;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ProtectedObject;
import com.linbit.linstor.transaction.TransactionObject;

import java.util.Map;
import java.util.UUID;

public interface VolumeGroup extends ProtectedObject, DbgInstanceUuid, TransactionObject, Comparable<VolumeGroup>
{
    UUID getUuid();

    ResourceGroup getResourceGroup();

    VolumeNumber getVolumeNumber();

    Props getProps(AccessContext accCtx)
        throws AccessDeniedException;

    void delete(AccessContext accCtx)
        throws AccessDeniedException, DatabaseException;

    VlmGrpApi getApiData(AccessContext accCtx)
        throws AccessDeniedException;

    interface VlmGrpApi
    {
        Integer getVolumeNr();
        Map<String, String> getProps();
        UUID getUUID();
    }

}
