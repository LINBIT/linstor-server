package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntApplySharedStorPoolLocksOuterClass.MsgIntApplySharedStorPoolLocks;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = InternalApiConsts.API_APPLY_SHARED_STOR_POOL_LOCKS,
    description = "Controller granted this satellite the requested shared StorPool locks"
)
@Singleton
public class ApplySharedStorPoolLocks implements ApiCall
{
    private final DeviceManager devMgr;

    @Inject
    public ApplySharedStorPoolLocks(DeviceManager devMgrRef)
    {
        devMgr = devMgrRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        devMgr.sharedStorPoolLocksGranted(
            MsgIntApplySharedStorPoolLocks.parseDelimitedFrom(msgDataIn).getSharedStorPoolLocksList()
        );
    }
}
