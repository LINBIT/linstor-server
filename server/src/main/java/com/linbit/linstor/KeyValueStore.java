package com.linbit.linstor;

import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.transaction.TransactionObject;

import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

public interface KeyValueStore extends TransactionObject, Comparable<KeyValueStore>
{
    UUID getUuid();

    ObjectProtection getObjProt();

    KeyValueStoreName getName();

    Props getProps(AccessContext accCtx)
        throws AccessDeniedException;

    boolean isDeleted();

    void delete(AccessContext accCtxRef)
        throws AccessDeniedException, SQLException;

    interface KvsApi
    {
        String getName();
        Map<String, String> getProps();
    }

    KvsApi getApiData(AccessContext accCtx, Long fullSyncId, Long updateId) throws AccessDeniedException;

    interface InitMaps
    {
        // currently only a place holder for future maps
    }
}
