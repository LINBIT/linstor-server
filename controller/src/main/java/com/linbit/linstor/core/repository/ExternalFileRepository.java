package com.linbit.linstor.core.repository;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.identifier.ExternalFileName;
import com.linbit.linstor.core.objects.ExternalFile;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ProtectedObject;

public interface ExternalFileRepository extends ProtectedObject
{
    void requireAccess(AccessContext accCtx, AccessType requested)
        throws AccessDeniedException;

    @Nullable
    ExternalFile get(AccessContext accCtx, ExternalFileName externalFileName)
        throws AccessDeniedException;

    void put(AccessContext accCtx, ExternalFile externalFile)
        throws AccessDeniedException;

    void remove(AccessContext accCtx, ExternalFileName externalFileName)
        throws AccessDeniedException;

    CoreModule.ExternalFileMap getMapForView(AccessContext accCtx)
        throws AccessDeniedException;
}
