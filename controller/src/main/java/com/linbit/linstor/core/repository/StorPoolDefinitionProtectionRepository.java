package com.linbit.linstor.core.repository;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;

import javax.inject.Inject;
import javax.inject.Singleton;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Holds the singleton stor pool definition map protection instance, allowing it to be initialized from the database
 * after dependency injection has been performed.
 */
@Singleton
public class StorPoolDefinitionProtectionRepository implements StorPoolDefinitionRepository
{
    private final CoreModule.StorPoolDefinitionMap storPoolDefinitionMap;
    private ObjectProtection storPoolDefinitionMapObjProt;

    // can't initialize objProt in constructor because of chicken-egg-problem
    @SuppressFBWarnings("NP_NONNULL_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR")
    @Inject
    public StorPoolDefinitionProtectionRepository(CoreModule.StorPoolDefinitionMap storPoolDefinitionMapRef)
    {
        storPoolDefinitionMap = storPoolDefinitionMapRef;
    }

    public void setObjectProtection(ObjectProtection storPoolDefinitionMapObjProtRef)
    {
        if (storPoolDefinitionMapObjProt != null)
        {
            throw new IllegalStateException("Object protection already set");
        }
        storPoolDefinitionMapObjProt = storPoolDefinitionMapObjProtRef;
    }

    @Override
    public ObjectProtection getObjProt()
    {
        checkProtSet();
        return storPoolDefinitionMapObjProt;
    }

    @Override
    public void requireAccess(AccessContext accCtx, AccessType requested)
        throws AccessDeniedException
    {
        checkProtSet();
        storPoolDefinitionMapObjProt.requireAccess(accCtx, requested);
    }

    @Override
    public @Nullable StorPoolDefinition get(
        AccessContext accCtx,
        StorPoolName storPoolName
    )
        throws AccessDeniedException
    {
        checkProtSet();
        storPoolDefinitionMapObjProt.requireAccess(accCtx, AccessType.VIEW);
        return storPoolDefinitionMap.get(storPoolName);
    }

    @Override
    public void put(AccessContext accCtx, StorPoolName storPoolName, StorPoolDefinition storPoolDefinition)
        throws AccessDeniedException
    {
        checkProtSet();
        storPoolDefinitionMapObjProt.requireAccess(accCtx, AccessType.CHANGE);
        storPoolDefinitionMap.put(storPoolName, storPoolDefinition);
    }

    @Override
    public void remove(AccessContext accCtx, StorPoolName storPoolName)
        throws AccessDeniedException
    {
        checkProtSet();
        storPoolDefinitionMapObjProt.requireAccess(accCtx, AccessType.CHANGE);
        storPoolDefinitionMap.remove(storPoolName);
    }

    @Override
    public CoreModule.StorPoolDefinitionMap getMapForView(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkProtSet();
        storPoolDefinitionMapObjProt.requireAccess(accCtx, AccessType.VIEW);
        return storPoolDefinitionMap;
    }

    private void checkProtSet()
    {
        if (storPoolDefinitionMapObjProt == null)
        {
            throw new IllegalStateException("Object protection not yet set");
        }
    }
}
