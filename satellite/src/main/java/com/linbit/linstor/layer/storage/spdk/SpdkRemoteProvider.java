package com.linbit.linstor.layer.storage.spdk;

import com.linbit.extproc.ExtCmdFactoryStlt;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.backupshipping.BackupShippingMgr;
import com.linbit.linstor.clone.CloneService;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.apicallhandler.StltExtToolsChecker;
import com.linbit.linstor.core.pojos.LocalPropsChangePojo;
import com.linbit.linstor.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.layer.storage.WipeHandler;
import com.linbit.linstor.layer.storage.spdk.utils.SpdkRemoteCommands;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.snapshotshipping.SnapshotShippingService;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import com.fasterxml.jackson.databind.JsonNode;

@Singleton
public class SpdkRemoteProvider extends AbsSpdkProvider<JsonNode>
{
    @Inject
    public SpdkRemoteProvider(
        ErrorReporter errorReporter,
        ExtCmdFactoryStlt extCmdFactory,
        @DeviceManagerContext AccessContext storDriverAccCtx,
        StltConfigAccessor stltConfigAccessor,
        WipeHandler wipeHandler,
        Provider<NotificationListener> notificationListenerProvider,
        Provider<TransactionMgr> transMgrProvider,
        SnapshotShippingService snapShipMrgRef,
        StltExtToolsChecker extToolsCheckerRef,
        CloneService cloneServiceRef,
        BackupShippingMgr backupShipMgrRef
    )
    {
        super(
            errorReporter,
            extCmdFactory,
            storDriverAccCtx,
            stltConfigAccessor,
            wipeHandler,
            notificationListenerProvider,
            transMgrProvider,
            "RemoteSPDK",
            DeviceProviderKind.REMOTE_SPDK,
            snapShipMrgRef,
            extToolsCheckerRef,
            new SpdkRemoteCommands(
                storDriverAccCtx,
                errorReporter,
                stltConfigAccessor
            ),
            cloneServiceRef,
            backupShipMgrRef
        );
    }

    @Override
    public DeviceProviderKind getDeviceProviderKind()
    {
        return DeviceProviderKind.REMOTE_SPDK;
    }

    @Override
    public LocalPropsChangePojo setLocalNodeProps(ReadOnlyProps localNodePropsRef)
        throws StorageException, AccessDeniedException
    {
        LocalPropsChangePojo changes = super.setLocalNodeProps(localNodePropsRef);
        ((SpdkRemoteCommands) spdkCommands).setLocalNodeProps(localNodePropsRef);
        return changes;
    }
}
