package com.linbit.linstor.backupshipping;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.CoreModule.RemoteMap;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.StltConnTracker;
import com.linbit.linstor.core.StltSecurityObjects;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.AbsRemote.RemoteType;
import com.linbit.linstor.core.objects.remotes.StltRemote;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.provider.AbsStorageVlmData;
import com.linbit.locks.LockGuardFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Map;
import java.util.function.BiConsumer;

@Singleton
public class BackupShippingL2LService extends AbsBackupShippingService
{
    public static final String SERVICE_INFO = "BackupShippingL2LService";
    private static final String DFLT_RESTORE_TIMEOUT_IN_MS = "300000";

    @Inject
    public BackupShippingL2LService(
        ErrorReporter errorReporterRef,
        ExtCmdFactory extCmdFactoryRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        CtrlStltSerializer interComSerializerRef,
        @SystemContext AccessContext accCtxRef,
        StltSecurityObjects stltSecObjRef,
        StltConfigAccessor stltConfigAccessorRef,
        StltConnTracker stltConnTrackerRef,
        RemoteMap remoteMapRef,
        LockGuardFactory lockGuardFactoryRef
    )
    {
        super(
            errorReporterRef,
            SERVICE_INFO,
            RemoteType.SATELLITE,
            extCmdFactoryRef,
            controllerPeerConnectorRef,
            interComSerializerRef,
            accCtxRef,
            stltSecObjRef,
            stltConfigAccessorRef,
            stltConnTrackerRef,
            remoteMapRef,
            lockGuardFactoryRef
        );
    }

    @Override
    protected String getCommandReceiving(String cmdRef, AbsRemote remoteRef, AbsStorageVlmData<Snapshot> snapVlmDataRef)
        throws AccessDeniedException
    {
        StltRemote stltRemote = (StltRemote) remoteRef;
        Map<String, Integer> ports = stltRemote.getPorts(accCtx);
        boolean useZstd = stltRemote.useZstd(accCtx);

        StringBuilder cmdBuilder = new StringBuilder()
            .append("set -o pipefail; ")
            .append("socat -dd TCP-LISTEN:")
            .append(ports.get(snapVlmDataRef.getVlmNr() + snapVlmDataRef.getRscLayerObject().getResourceNameSuffix()))
            .append(" STDOUT | ");
        if (useZstd)
        {
            cmdBuilder.append("zstd -d | ");
        }
        // "pv -s 100m -bnr -i 0.1 | " +
        cmdBuilder.append(cmdRef);

        return cmdBuilder.toString();
    }

    @Override
    protected String getCommandSending(String cmdRef, AbsRemote remoteRef, AbsStorageVlmData<Snapshot> snapVlmDataRef)
        throws AccessDeniedException
    {
        StltRemote stltRemote = (StltRemote) remoteRef;

        boolean useZstd = stltRemote.useZstd(accCtx);
        Map<String, Integer> ports = stltRemote.getPorts(accCtx);

        StringBuilder cmdBuilder = new StringBuilder()
            .append("set -o pipefail; ")
            .append(cmdRef)
            .append(" | ");
        if (useZstd)
        {
            cmdBuilder.append("zstd | ");
        }
        // "pv -s 100m -bnr -i 0.1 | " +
        cmdBuilder.append("socat STDIN TCP:")
            .append(stltRemote.getIp(accCtx))
            .append(":")
            .append(ports.get(snapVlmDataRef.getVlmNr() + snapVlmDataRef.getRscLayerObject().getResourceNameSuffix()));

        return cmdBuilder.toString();
    }

    @Override
    protected BackupShippingDaemon createDaemon(
        AbsStorageVlmData<Snapshot> snapVlmDataRef,
        String[] fullCommandRef,
        String backupNameRef,
        AbsRemote remoteRef,
        boolean restoreRef,
        Integer portRef,
        BiConsumer<Boolean, Integer> postActionRef
    )
    {
        return new BackupShippingL2LDaemon(
            errorReporter,
            threadGroup,
            backupNameRef,
            fullCommandRef,
            portRef,
            postActionRef,
            getRestoreTimeout(snapVlmDataRef, restoreRef)
        );
    }

    private @Nullable Long getRestoreTimeout(AbsStorageVlmData<Snapshot> snapVlmDataRef, boolean restoreRef)
        throws ImplementationError
    {
        @Nullable Long restoreTimeoutMs = null;
        if (restoreRef)
        {
            Snapshot snapshot = snapVlmDataRef.getRscLayerObject().getAbsResource();
            PriorityProps prioProps;
            try
            {
                prioProps = new PriorityProps(
                    snapshot.getNode().getProps(accCtx),
                    snapshot.getResourceDefinition().getProps(accCtx),
                    snapshot.getResourceDefinition().getResourceGroup().getProps(accCtx),
                    stltConfigAccessor.getReadonlyProps(ApiConsts.NAMESPC_BACKUP_SHIPPING)
                );
                Long timeout = Long.parseLong(
                    prioProps.getProp(
                        ApiConsts.KEY_RECV_TIMEOUT_IN_MS,
                        ApiConsts.NAMESPC_BACKUP_SHIPPING,
                        DFLT_RESTORE_TIMEOUT_IN_MS
                    )
                );
                if (timeout >= 0)
                {
                    restoreTimeoutMs = timeout;
                }
            }
            catch (AccessDeniedException exc)
            {
                throw new ImplementationError(exc);
            }
        }
        return restoreTimeoutMs;
    }

    @Override
    protected String getBackupNameForRestore(AbsStorageVlmData<Snapshot> snapVlmDataRef)
        throws InvalidKeyException, AccessDeniedException
    {
        return snapVlmDataRef.getIdentifier();
    }

    @Override
    protected boolean preCtrlNotifyBackupShipped(
        boolean successRef,
        boolean restoringRef,
        Snapshot snapRef,
        ShippingInfo shippingInfoRef
    )
    {
        return successRef;
    }

    @Override
    protected void postAllBackupPartsRegistered(Snapshot snap, ShippingInfo info)
    {
        String remoteName = "";
        synchronized (snap)
        {
            try
            {
                if (snap.getFlags().isSet(accCtx, Snapshot.Flags.BACKUP_TARGET))
                {
                    remoteName = snap.getProps(accCtx).getProp(
                        InternalApiConsts.KEY_BACKUP_SRC_REMOTE,
                        ApiConsts.NAMESPC_BACKUP_SHIPPING
                    );
                    if (remoteName == null || remoteName.isEmpty())
                    {
                        throw new ImplementationError(
                            "KEY_BACKUP_SRC_REMOTE is not set for the backup-shipping of " +
                                snap.getSnapshotName()
                        );
                    }
                }
                else
                {
                    return;
                }
            }
            catch (InvalidKeyException | AccessDeniedException exc)
            {
                throw new ImplementationError(exc);
            }
            // wait to see if the daemons could start successfully
            try
            {
                snap.wait(750);
            }
            catch (InterruptedException exc)
            {
                // ignore if this happens during shutdown
                throw new ImplementationError(exc);
            }
        }
        if (info.snapVlmDataFinishedShipping > 0)
        {
            killAllShipping(true);
        }
        else
        {

            // tell ctrl to tell src to start sending
            controllerPeerConnector.getControllerPeer().sendMessage(
                interComSerializer
                    .onewayBuilder(InternalApiConsts.API_NOTIFY_BACKUP_RCV_READY)
                    .notifyBackupRcvReady(
                        remoteName,
                        snap.getSnapshotName().displayValue,
                        snap.getResourceName().displayValue,
                        snap.getNodeName().displayValue
                    )
                    .build(),
                InternalApiConsts.API_NOTIFY_BACKUP_RCV_READY
            );
        }
    }
}
