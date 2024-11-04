package com.linbit.linstor.core.repository;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.identifier.KeyValueStoreName;
import com.linbit.linstor.core.objects.KeyValueStore;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;

import javax.inject.Inject;
import javax.inject.Singleton;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Holds the singleton KeyValueStore map protection instance, allowing it to be initialized from the database
 * after dependency injection has been performed.
 */
@Singleton
public class KeyValueStoreProtectionRepository implements KeyValueStoreRepository
{
    private final CoreModule.KeyValueStoreMap kvsMap;
    private ObjectProtection kvsMapObjProt;

    // can't initialize objProt in constructor because of chicken-egg-problem
    @SuppressFBWarnings("NP_NONNULL_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR")
    @Inject
    public KeyValueStoreProtectionRepository(CoreModule.KeyValueStoreMap kvsMapRef)
    {
        kvsMap = kvsMapRef;
    }

    public void setObjectProtection(ObjectProtection kvsMapObjProtRef)
    {
        if (kvsMapObjProt != null)
        {
            throw new IllegalStateException("Object protection already set");
        }
        kvsMapObjProt = kvsMapObjProtRef;
    }

    @Override
    public ObjectProtection getObjProt()
    {
        checkProtSet();
        return kvsMapObjProt;
    }

    @Override
    public void requireAccess(AccessContext accCtx, AccessType requested)
        throws AccessDeniedException
    {
        checkProtSet();
        kvsMapObjProt.requireAccess(accCtx, requested);
    }

    @Override
    public @Nullable KeyValueStore get(
        AccessContext accCtx,
        KeyValueStoreName kvsName
    )
        throws AccessDeniedException
    {
        checkProtSet();
        kvsMapObjProt.requireAccess(accCtx, AccessType.VIEW);
        return kvsMap.get(kvsName);
    }

    @Override
    public void put(AccessContext accCtx, KeyValueStoreName kvsName, KeyValueStore kvs)
        throws AccessDeniedException
    {
        checkProtSet();
        kvsMapObjProt.requireAccess(accCtx, AccessType.CHANGE);
        kvsMap.put(kvsName, kvs);
    }

    @Override
    public void remove(AccessContext accCtx, KeyValueStoreName kvsName)
        throws AccessDeniedException
    {
        checkProtSet();
        kvsMapObjProt.requireAccess(accCtx, AccessType.CHANGE);
        kvsMap.remove(kvsName);
    }

    @Override
    public CoreModule.KeyValueStoreMap getMapForView(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkProtSet();
        kvsMapObjProt.requireAccess(accCtx, AccessType.VIEW);
        return kvsMap;
    }

    private void checkProtSet()
    {
        if (kvsMapObjProt == null)
        {
            throw new IllegalStateException("Object protection not yet set");
        }
    }
}
