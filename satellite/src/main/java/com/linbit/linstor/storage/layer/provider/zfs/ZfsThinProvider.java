package com.linbit.linstor.storage.layer.provider.zfs;

import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.layer.DeviceLayer.NotificationListener;

public class ZfsThinProvider extends ZfsProvider
{
    public ZfsThinProvider(
        ErrorReporter errorReporter,
        ExtCmdFactory extCmdFactory,
        AccessContext storDriverAccCtx,
        NotificationListener notificationListenerRef
    )
    {
        super(errorReporter, extCmdFactory, storDriverAccCtx, notificationListenerRef);
    }
}
