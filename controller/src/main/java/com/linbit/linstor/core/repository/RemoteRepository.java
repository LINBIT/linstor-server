package com.linbit.linstor.core.repository;

import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.objects.remotes.Remote;
import com.linbit.linstor.core.objects.remotes.S3Remote;
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

    default S3Remote getS3(AccessContext accCtx, RemoteName remoteName) throws AccessDeniedException
    {
        Remote remote = get(accCtx, remoteName);
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
