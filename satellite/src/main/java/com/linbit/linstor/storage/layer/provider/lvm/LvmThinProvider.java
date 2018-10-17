package com.linbit.linstor.storage.layer.provider.lvm;

import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.storage.layer.provider.StorageLayer;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Provider;

public class LvmThinProvider extends LvmProvider
{
    public LvmThinProvider(
        ExtCmdFactory extCmdFactory,
        AccessContext storDriverAccCtx,
        StltConfigAccessor stltConfigAccessor,
        Provider<TransactionMgr> transMgrProviderRef,
        NotificationListener notificationListenerRef,
        StorageLayer storageLayerRef,
        ErrorReporter errorReporterRef
    )
    {
        super(
            extCmdFactory,
            storDriverAccCtx,
            stltConfigAccessor,
            transMgrProviderRef,
            notificationListenerRef,
            storageLayerRef,
            errorReporterRef
        );
    }
}
