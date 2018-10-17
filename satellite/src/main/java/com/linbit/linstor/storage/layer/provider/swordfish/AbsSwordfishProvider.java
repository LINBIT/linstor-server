package com.linbit.linstor.storage.layer.provider.swordfish;

import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.storage.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.storage.layer.provider.DeviceProvider;

public abstract class AbsSwordfishProvider implements DeviceProvider
{
    protected final NotificationListener notificationListener;
    protected Props localNodeProps;

    public AbsSwordfishProvider(NotificationListener notificationListenerRef)
    {
        notificationListener = notificationListenerRef;
    }

    public void setLocalNodeProps(Props localNodePropsRef)
    {
        localNodeProps = localNodePropsRef;
    }
}
