package com.linbit.linstor.storage.layer.provider.swordfish;

import com.linbit.linstor.SnapshotVolume;
import com.linbit.linstor.Volume;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessDeniedException;

import java.sql.SQLException;
import java.util.List;

import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.storage.layer.provider.DeviceProvider;

import javax.inject.Provider;

public abstract class AbsSwordfishProvider implements DeviceProvider
{
    protected final Provider<NotificationListener> notificationListenerProvider;
    protected final String driverName;
    protected final String createdMsg;
    protected final String deletedMsg;
    protected Props localNodeProps;

    public AbsSwordfishProvider(
        Provider<NotificationListener> notificationListenerProviderRef,
        String driverNameRef,
        String createdMsgRef,
        String deletedMsgRef
    )
    {
        notificationListenerProvider = notificationListenerProviderRef;
        driverName = driverNameRef;
        createdMsg = createdMsgRef;
        deletedMsg = deletedMsgRef;
    }

    public void setLocalNodeProps(Props localNodePropsRef)
    {
        localNodeProps = localNodePropsRef;
    }

    @Override
    public void clearCache()
    {
        // no-op
    }

    @Override
    public void prepare(List<Volume> volumes, List<SnapshotVolume> snapVlms)
        throws StorageException, AccessDeniedException, SQLException
    {
        // no-op
    }

    @Override
    public abstract void process(List<Volume> volumes, List<SnapshotVolume> snapVolumes, ApiCallRcImpl apiCallRc);

    protected void addCreatedMsg(Volume vlm, ApiCallRcImpl apiCallRc)
    {
        apiCallRc.addEntry(
            ApiCallRcImpl.simpleEntry(
                ApiConsts.MASK_VLM | ApiConsts.CREATED,
                "Volume for " + vlm.getResource().toString() + " [" + driverName + "]" + createdMsg
            )
        );
    }

    protected void addDeletedMsg(Volume vlm, ApiCallRcImpl apiCallRc)
    {
        apiCallRc.addEntry(
            ApiCallRcImpl.simpleEntry(
                ApiConsts.MASK_VLM | ApiConsts.DELETED,
                "Volume for " + vlm.getResource().toString() + " [" + driverName + "]" + deletedMsg
            )
        );
    }

}
