package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.drbd.CbtSessionService;
import com.linbit.linstor.storage.StorageException;
import com.linbit.locks.LockGuardFactory;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

@ProtobufApiCall(
    name = InternalApiConsts.API_REQUEST_CBT_SESSION_CLOSE,
    description = "Closes the snapshot of a CBT session."
)
@Singleton
public class ReqCbtSessionClose extends ReqCbtSession
{
    @Inject
    public ReqCbtSessionClose(
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        CbtSessionService cbtSessionServiceRef,
        CommonSerializer commonSerializerRef,
        @Named(ApiModule.API_CALL_ID) Provider<Long> apiCallIdProviderRef
    )
    {
        super(
            scopeRunnerRef,
            lockGuardFactoryRef,
            cbtSessionServiceRef,
            commonSerializerRef,
            apiCallIdProviderRef,
            InternalApiConsts.API_REQUEST_CBT_SESSION_CLOSE
        );
    }

    @Override
    protected void execute(CbtSessionService serviceRef, ResourceName rscNameRef, SnapshotName snapNameRef)
        throws StorageException, DatabaseException
    {
        serviceRef.close(rscNameRef, snapNameRef);
    }
}
