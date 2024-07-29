package com.linbit.linstor.core.repository;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.identifier.KeyValueStoreName;
import com.linbit.linstor.core.objects.KeyValueStore;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ProtectedObject;

/**
 * Provides access to {@link KeyValueStore}s with automatic security checks.
 */
public interface KeyValueStoreRepository extends ProtectedObject
{
    void requireAccess(AccessContext accCtx, AccessType requested)
        throws AccessDeniedException;

    @Nullable
    KeyValueStore get(AccessContext accCtx, KeyValueStoreName nameRef)
        throws AccessDeniedException;

    void put(AccessContext accCtx, KeyValueStoreName kvsName, KeyValueStore kvs)
        throws AccessDeniedException;

    void remove(AccessContext accCtx, KeyValueStoreName kvsName)
        throws AccessDeniedException;

    CoreModule.KeyValueStoreMap getMapForView(AccessContext accCtx)
        throws AccessDeniedException;
}
