package com.linbit.linstor.core.repository;

import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.objects.Remote;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ProtectedObject;

public interface RemoteRepository extends ProtectedObject
{
    void requireAccess(AccessContext accCtx, AccessType requested) throws AccessDeniedException;

    Remote get(AccessContext accCtx, RemoteName remoteName) throws AccessDeniedException;

    void put(AccessContext accCtx, Remote remote) throws AccessDeniedException;

    void remove(AccessContext accCtx, RemoteName remoteName) throws AccessDeniedException;

    CoreModule.RemoteMap getMapForView(AccessContext accCtx) throws AccessDeniedException;
}
