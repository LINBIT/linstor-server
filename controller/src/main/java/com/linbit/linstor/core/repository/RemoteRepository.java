package com.linbit.linstor.core.repository;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.S3Remote;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ProtectedObject;

public interface RemoteRepository extends ProtectedObject
{
    void requireAccess(AccessContext accCtx, AccessType requested) throws AccessDeniedException;

    @Nullable
    AbsRemote get(AccessContext accCtx, RemoteName remoteName) throws AccessDeniedException;

    void put(AccessContext accCtx, AbsRemote remote) throws AccessDeniedException;

    void remove(AccessContext accCtx, RemoteName remoteName) throws AccessDeniedException;

    CoreModule.RemoteMap getMapForView(AccessContext accCtx) throws AccessDeniedException;

    default @Nullable S3Remote getS3(AccessContext accCtx, RemoteName remoteName) throws AccessDeniedException
    {
        AbsRemote remote = get(accCtx, remoteName);
        if (remote != null && !(remote instanceof S3Remote))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_REMOTE_NAME,
                    "Given Remote was not of class S3Remote but of " + remote.getClass().getCanonicalName()
                )
            );
        }
        return (S3Remote) remote;
    }
}
