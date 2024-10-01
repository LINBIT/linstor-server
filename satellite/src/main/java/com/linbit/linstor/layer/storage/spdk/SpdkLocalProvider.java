package com.linbit.linstor.layer.storage.spdk;

import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactoryStlt;
import com.linbit.fsevent.FileSystemWatch;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.backupshipping.BackupShippingMgr;
import com.linbit.linstor.clone.CloneService;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.apicallhandler.StltExtToolsChecker;
import com.linbit.linstor.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.layer.storage.WipeHandler;
import com.linbit.linstor.layer.storage.spdk.utils.SpdkLocalCommands;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.snapshotshipping.SnapshotShippingService;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

@Singleton
public class SpdkLocalProvider extends AbsSpdkProvider<OutputData>
{
    @Inject
    public SpdkLocalProvider(
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
        BackupShippingMgr backupShipMgrRef,
        FileSystemWatch fileSystemWatchRef
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
            "SPDK",
            DeviceProviderKind.SPDK,
            snapShipMrgRef,
            extToolsCheckerRef,
            new SpdkLocalCommands(extCmdFactory),
            cloneServiceRef,
            backupShipMgrRef,
            fileSystemWatchRef
        );
    }

    @Override
    public DeviceProviderKind getDeviceProviderKind()
    {
        return DeviceProviderKind.SPDK;
    }
}
