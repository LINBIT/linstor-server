package com.linbit.linstor.core.repository;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.RemoteMap;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;

import javax.inject.Inject;
import javax.inject.Singleton;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@Singleton
public class RemoteProtectionRepository implements RemoteRepository
{
    private final CoreModule.RemoteMap remoteMap;
    private ObjectProtection remoteMapObjProt;

    // can't initialize objProt in constructor because of chicken-egg-problem
    @SuppressFBWarnings("NP_NONNULL_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR")
    @Inject
    public RemoteProtectionRepository(CoreModule.RemoteMap remoteMapRef)
    {
        remoteMap = remoteMapRef;
    }

    public void setObjectProtection(ObjectProtection remoteMapObjProtRef)
    {
        if (remoteMapObjProt != null)
        {
            throw new IllegalStateException("Object protection already set");
        }
        remoteMapObjProt = remoteMapObjProtRef;
    }

    @Override
    public ObjectProtection getObjProt()
    {
        checkProtSet();
        return remoteMapObjProt;
    }

    @SuppressFBWarnings("UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR")
    @Override
    public void requireAccess(AccessContext accCtx, AccessType requested) throws AccessDeniedException
    {
        // spotbugs-warning suppressed, because checkProtSet() does exactly what is needed
        checkProtSet();
        remoteMapObjProt.requireAccess(accCtx, requested);
    }

    @Override
    public @Nullable AbsRemote get(AccessContext accCtx, RemoteName remoteNameRef) throws AccessDeniedException
    {
        checkProtSet();
        requireAccess(accCtx, AccessType.VIEW);
        return remoteMap.get(remoteNameRef);
    }

    @Override
    public void put(AccessContext accCtx, AbsRemote remoteRef) throws AccessDeniedException
    {
        checkProtSet();
        requireAccess(accCtx, AccessType.CHANGE);
        remoteMap.put(remoteRef.getName(), remoteRef);
    }

    @Override
    public void remove(AccessContext accCtx, RemoteName remoteNameRef) throws AccessDeniedException
    {
        checkProtSet();
        requireAccess(accCtx, AccessType.CHANGE);
        remoteMap.remove(remoteNameRef);
    }

    @Override
    public RemoteMap getMapForView(AccessContext accCtx) throws AccessDeniedException
    {
        checkProtSet();
        requireAccess(accCtx, AccessType.VIEW);
        return remoteMap;
    }

    private void checkProtSet()
    {
        if (remoteMapObjProt == null)
        {
            throw new IllegalStateException("Object protection not yet set");
        }
    }
}
