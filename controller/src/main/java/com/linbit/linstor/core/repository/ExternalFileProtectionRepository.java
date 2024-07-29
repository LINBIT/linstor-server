package com.linbit.linstor.core.repository;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.ExternalFileMap;
import com.linbit.linstor.core.identifier.ExternalFileName;
import com.linbit.linstor.core.objects.ExternalFile;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class ExternalFileProtectionRepository implements ExternalFileRepository
{
    private final CoreModule.ExternalFileMap extFileMap;
    private @Nullable ObjectProtection extFileMapObjProt;

    @Inject
    public ExternalFileProtectionRepository(CoreModule.ExternalFileMap extFileMapRef)
    {
        extFileMap = extFileMapRef;
    }

    public void setObjectProtection(ObjectProtection extFileMapObjProtRef)
    {
        if (extFileMapObjProt != null)
        {
            throw new IllegalStateException("Object protection already set");
        }
        extFileMapObjProt = extFileMapObjProtRef;
    }

    @Override
    public @Nullable ObjectProtection getObjProt()
    {
        checkProtSet();
        return extFileMapObjProt;
    }

    @Override
    public void requireAccess(AccessContext accCtx, AccessType requested)
        throws AccessDeniedException
    {
        checkProtSet();
        extFileMapObjProt.requireAccess(accCtx, requested);
    }

    @Override
    public @Nullable ExternalFile get(AccessContext accCtx, ExternalFileName externalFileNameRef)
        throws AccessDeniedException
    {
        checkProtSet();
        extFileMapObjProt.requireAccess(accCtx, AccessType.VIEW);
        return extFileMap.get(externalFileNameRef);
    }

    @Override
    public void put(AccessContext accCtx, ExternalFile externalFileRef) throws AccessDeniedException
    {
        checkProtSet();
        extFileMapObjProt.requireAccess(accCtx, AccessType.CHANGE);
        extFileMap.put(externalFileRef.getName(), externalFileRef);
    }

    @Override
    public void remove(AccessContext accCtx, ExternalFileName externalFileNameRef) throws AccessDeniedException
    {
        checkProtSet();
        extFileMapObjProt.requireAccess(accCtx, AccessType.CHANGE);
        extFileMap.remove(externalFileNameRef);
    }

    @Override
    public ExternalFileMap getMapForView(AccessContext accCtx) throws AccessDeniedException
    {
        checkProtSet();
        extFileMapObjProt.requireAccess(accCtx, AccessType.VIEW);
        return extFileMap;
    }

    private void checkProtSet()
    {
        if (extFileMapObjProt == null)
        {
            throw new IllegalStateException("Object protection not yet set");
        }
    }

}
