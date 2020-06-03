package com.linbit.linstor.snapshotshipping;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.provider.AbsStorageVlmData;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;

@Singleton
public class SnapshotShippingManager
{
    private final AccessContext storDriverAccCtx;
    private final ExtCmdFactory extCmdFactory;
    private final ErrorReporter errorReporter;
    private final ControllerPeerConnector controllerPeerConnector;
    private final CtrlStltSerializer interComSerializer;

    private final TreeMap<Snapshot, ShippingInfo> shippingInfoMap;

    @Inject
    public SnapshotShippingManager(
        @SystemContext AccessContext storDriverAccCtxRef,
        ExtCmdFactory extCmdFactoryRef,
        ErrorReporter errorReporterRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        CtrlStltSerializer interComSerializerRef
    )
    {
        storDriverAccCtx = storDriverAccCtxRef;
        extCmdFactory = extCmdFactoryRef;
        errorReporter = errorReporterRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        interComSerializer = interComSerializerRef;

        shippingInfoMap = new TreeMap<>();
    }

    public void startReceiving(
        String shippingDescr,
        String snapshotShippingReceivingCommandRef,
        String port,
        AbsStorageVlmData<Snapshot> snapVlmData
    )
        throws StorageException
    {
        startDaemon(
            snapshotShippingReceivingCommandRef,
            new String[]
            {
                "bash",
                "-c",
                "socat TCP-LISTEN:" + port + " STDOUT | " +
                    "zstd -d | " +
                    // "pv -s 100m -bnr -i 0.1 | " +
                    snapshotShippingReceivingCommandRef
            },
            shippingDescr,
            success -> postShipping(
                success,
                snapVlmData,
                InternalApiConsts.API_NOTIFY_SNAPSHOT_SHIPPING_RECEIVED
            ), snapVlmData
        );
    }

    public void startSending(
        String shippingDescr,
        String snapshotShippingSendingCommandRef,
        NetInterface targetNetIfRef,
        String socatPortRef,
        AbsStorageVlmData<Snapshot> snapVlmData
    )
        throws AccessDeniedException, StorageException
    {
        startDaemon(
            snapshotShippingSendingCommandRef,
            new String[]
            {
                "bash",
                "-c",
                snapshotShippingSendingCommandRef + " | " +
                // "pv -s 100m -bnr -i 0.1 | " +
                "zstd | " +
                "socat STDIN TCP:" + targetNetIfRef.getAddress(storDriverAccCtx).getAddress() + ":" + socatPortRef
            },
            shippingDescr,
            // success -> postShipping(
            // success,
            // snapVlmData,
            // InternalApiConsts.API_NOTIFY_SNAPSHOT_SHIPPING_SENT
            // ),
            success ->
            {}, // noop - for now?
            snapVlmData
        );
    }

    private void startDaemon(
        String sendRecvCommand,
        String[] fullCommand,
        String shippingDescr,
        Consumer<Boolean> postAction,
        AbsStorageVlmData<Snapshot> snapVlmData
    )
        throws StorageException
    {
        if (!alreadyStarted(snapVlmData))
        {
            killIfRunning(sendRecvCommand);

            SnapshotShippingDaemon daemon = new SnapshotShippingDaemon(
                errorReporter,
                "shipping_" + shippingDescr,
                fullCommand,
                postAction
            );
            Snapshot snap = snapVlmData.getRscLayerObject().getAbsResource();
            ShippingInfo info = shippingInfoMap.get(snap);
            if (info == null)
            {
                info = new ShippingInfo();
                shippingInfoMap.put(snap, info);
            }
            info.snapVlmDataInfoMap.put(snapVlmData, new SnapVlmDataInfo(daemon));
        }
    }

    private void postShipping(
        boolean successRef,
        AbsStorageVlmData<Snapshot> snapVlmData,
        String internalApiName
    )
    {
        Snapshot snap = snapVlmData.getRscLayerObject().getAbsResource();
        ShippingInfo shippingInfo = shippingInfoMap.get(snap);

        synchronized (snap)
        {
            shippingInfo.snapVlmDataFinishedShipping++;
            if (successRef)
            {
                shippingInfo.snapVlmDataFinishedSuccessfully++;
            }
            if (shippingInfo.snapVlmDataFinishedShipping == shippingInfo.snapVlmDataInfoMap.size())
            {
                boolean success = shippingInfo.snapVlmDataFinishedSuccessfully == shippingInfo.snapVlmDataFinishedShipping;
                controllerPeerConnector.getControllerPeer().sendMessage(
                    interComSerializer
                        .onewayBuilder(internalApiName)
                        .notifySnapshotShipped(snap, success)
                        .build()
                );
            }
        }
    }

    public void allSnapshotPartsRegistered(Snapshot snap)
    {
        ShippingInfo info = shippingInfoMap.get(snap);
        if (info != null)
        {
            synchronized (info)
            {
                if (!info.isStarted)
                {
                    for (SnapVlmDataInfo snapVlmDataInfo : info.snapVlmDataInfoMap.values())
                    {
                        snapVlmDataInfo.daemon.start();
                    }
                    info.isStarted = true;
                }
            }
        }
    }

    private boolean alreadyStarted(AbsStorageVlmData<Snapshot> snapVlmDataRef)
    {
        ShippingInfo shippingInfo = shippingInfoMap.get(snapVlmDataRef.getVolume().getAbsResource());
        return shippingInfo != null && shippingInfo.snapVlmDataInfoMap.containsKey(snapVlmDataRef);
    }

    private void killIfRunning(String cmdToKill) throws StorageException
    {
        try
        {
            OutputData outputData = extCmdFactory.create().exec(
                "bash",
                "-c",
                "ps a -o pid,command | grep '" + cmdToKill + "'"
            );
            if (outputData.exitCode == 0) // != 0 means grep didnt find anything
            {
                String out = new String(outputData.stdoutData);
                String[] lines = out.split("\n");
                for (String line : lines)
                {
                    line = line.trim(); // ps prints a trailing space
                    String pid = line.substring(0, line.indexOf(" "));
                    extCmdFactory.create().exec("kill", pid);
                }
                Thread.sleep(500); // wait a bit so not just the process is killed but also the socket is closed
            }
        }
        catch (ChildProcessTimeoutException | IOException | InterruptedException exc)
        {
            throw new StorageException("Failed to determine if command is still running: " + cmdToKill, exc);
        }
    }

    private static class ShippingInfo
    {
        private boolean isStarted = false;
        private Map<AbsStorageVlmData<Snapshot>, SnapVlmDataInfo> snapVlmDataInfoMap = new HashMap<>();

        private int snapVlmDataFinishedShipping = 0;
        private int snapVlmDataFinishedSuccessfully = 0;
    }

    private static class SnapVlmDataInfo
    {
        private SnapshotShippingDaemon daemon;

        private SnapVlmDataInfo(SnapshotShippingDaemon daemonRef)
        {
            daemon = daemonRef;
        }
    }
}
