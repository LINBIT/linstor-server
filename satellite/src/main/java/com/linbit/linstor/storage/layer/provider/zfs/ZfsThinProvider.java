package com.linbit.linstor.storage.layer.provider.zfs;

import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.layer.DeviceLayer.NotificationListener;

public class ZfsThinProvider extends ZfsProvider
{
    public ZfsThinProvider(
        ExtCmdFactory extCmdFactory,
        AccessContext storDriverAccCtx,
        NotificationListener notificationListenerRef
    )
    {
        super(extCmdFactory, storDriverAccCtx, notificationListenerRef);
    }
}
