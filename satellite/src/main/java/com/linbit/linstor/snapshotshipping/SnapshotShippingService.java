package com.linbit.linstor.snapshotshipping;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;

@Singleton
public class SnapshotShippingService implements SystemService
{
    public static final ServiceName SERVICE_NAME;
    public static final String SERVICE_INFO = "SnapshotShippingService";

    private static final String CMD_FORMAT_RECEIVING =
        "set -o pipefail; socat TCP-LISTEN:%s STDOUT | zstd -d | " +
        // "pv -s 100m -bnr -i 0.1 | " +
        "%s";
    private static final String CMD_FORMAT_SENDING =
        "%s | " +
        // "pv -s 100m -bnr -i 0.1 | " +
        "zstd | socat STDIN TCP:%s:%s";

    private final AccessContext storDriverAccCtx;
    private final ExtCmdFactory extCmdFactory;
    private final ErrorReporter errorReporter;
    private final ControllerPeerConnector controllerPeerConnector;
    private final CtrlStltSerializer interComSerializer;

    private final Map<Snapshot, ShippingInfo> shippingInfoMap;
    private final ThreadGroup threadGroup;

    private ServiceName instanceName;
    private boolean serviceStarted = false;

    static
    {
        try
        {
            SERVICE_NAME = new ServiceName(SERVICE_INFO);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new ImplementationError(invalidNameExc);
        }
    }

    @Inject
    public SnapshotShippingService(
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

        try
        {
            instanceName = new ServiceName(SERVICE_INFO);
        }
        catch (InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }

        shippingInfoMap = Collections.synchronizedMap(new TreeMap<>());
        threadGroup = new ThreadGroup("SnapshotShippingSerivceThreadGroup");
    }

    public void killAllShipping() throws StorageException
    {
        killIfRunning(
            "^\\s*[0-9]+\\s+bash -c " + String.format(CMD_FORMAT_RECEIVING, "[0-9]+", ".+").replaceAll("[|]", "[|]") + "$"
        );
        killIfRunning(
            "^\\s*[0-9]+\\s+bash -c " + String.format(CMD_FORMAT_SENDING, ".+", "[0-9.]+", "[0-9]+").replaceAll("[|]", "[|]") + "$"
        );
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
                String.format(CMD_FORMAT_RECEIVING, port, snapshotShippingReceivingCommandRef)
            },
            shippingDescr,
            success -> postShipping(
                success,
                snapVlmData,
                InternalApiConsts.API_NOTIFY_SNAPSHOT_SHIPPING_RECEIVED,
                true
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
                String.format(
                    CMD_FORMAT_SENDING,
                    snapshotShippingSendingCommandRef,
                    targetNetIfRef.getAddress(storDriverAccCtx).getAddress(),
                    socatPortRef
                )
            },
            shippingDescr,
            // success -> postShipping(
            // success,
            // snapVlmData,
            // InternalApiConsts.API_NOTIFY_SNAPSHOT_SHIPPING_SENT
            // ),
            success -> postShipping(
                success,
                snapVlmData,
                null,
                false
            ),
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
        if (serviceStarted)
        {
            if (!alreadyStarted(snapVlmData))
            {
                killIfRunning(sendRecvCommand);

                SnapshotShippingDaemon daemon = new SnapshotShippingDaemon(
                    errorReporter,
                    threadGroup,
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
        else
        {
            throw new StorageException("SnapshotShippingService not started");
        }
    }

    private void postShipping(
        boolean successRef,
        AbsStorageVlmData<Snapshot> snapVlmData,
        String internalApiName,
        boolean updateCtrlRef
    )
    {
        Snapshot snap = snapVlmData.getRscLayerObject().getAbsResource();
        synchronized (snap)
        {
            ShippingInfo shippingInfo = shippingInfoMap.get(snap);
            /*
             * shippingInfo might be already null as we delete it at the end of this method.
             * this postShipping will be called twice, from stdErr and from stdOut proxy
             * thread.
             */
            if (shippingInfo != null)
            {
                shippingInfo.snapVlmDataFinishedShipping++;
                if (successRef)
                {
                    shippingInfo.snapVlmDataFinishedSuccessfully++;
                }
                if (shippingInfo.snapVlmDataFinishedShipping == shippingInfo.snapVlmDataInfoMap.size())
                {
                    if (updateCtrlRef)
                    {
                        boolean success = shippingInfo.snapVlmDataFinishedSuccessfully == shippingInfo.snapVlmDataFinishedShipping;
                        controllerPeerConnector.getControllerPeer().sendMessage(
                            interComSerializer.onewayBuilder(internalApiName).notifySnapshotShipped(snap, success).build()
                        );
                    }
                    shippingInfoMap.remove(snap);
                }
            }
        }
    }

    public void allSnapshotPartsRegistered(Snapshot snap)
    {
        synchronized (snap)
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
                "ps ax -o pid,command | grep -E '" + cmdToKill + "' | grep -v grep"
            );
            if (outputData.exitCode == 0) // != 0 means grep didnt find anything
            {
                String out = new String(outputData.stdoutData);
                String[] lines = out.split("\n");
                for (String line : lines)
                {
                    line = line.trim(); // ps prints a trailing space
                    String pid = line.substring(0, line.indexOf(" "));
                    extCmdFactory.create().exec("pkill", "-9", "--parent", pid);
                    // extCmdFactory.create().exec("kill", pid);
                }
                Thread.sleep(500); // wait a bit so not just the process is killed but also the socket is closed
            }
        }
        catch (ChildProcessTimeoutException | IOException | InterruptedException exc)
        {
            throw new StorageException("Failed to determine if command is still running: " + cmdToKill, exc);
        }
    }

    @Override
    public ServiceName getServiceName()
    {
        return SERVICE_NAME;
    }

    @Override
    public String getServiceInfo()
    {
        return SERVICE_INFO;
    }

    @Override
    public ServiceName getInstanceName()
    {
        return instanceName;
    }

    @Override
    public void setServiceInstanceName(ServiceName instanceNameRef)
    {
        instanceName = instanceNameRef;
    }

    @Override
    public boolean isStarted()
    {
        return serviceStarted;
    }

    @Override
    public void start() throws SystemServiceStartException
    {
        serviceStarted = true;
    }

    @Override
    public void shutdown()
    {
        serviceStarted = false;
        for (ShippingInfo info : shippingInfoMap.values())
        {
            for (SnapVlmDataInfo snapVlmDataInfo : info.snapVlmDataInfoMap.values())
            {
                snapVlmDataInfo.daemon.shutdown();
            }
        }
    }

    @Override
    public void awaitShutdown(long timeoutRef) throws InterruptedException
    {
        long exitTime = Math.addExact(System.currentTimeMillis(), timeoutRef);
        for (ShippingInfo info : shippingInfoMap.values())
        {
            for (SnapVlmDataInfo snapVlmDataInfo : info.snapVlmDataInfoMap.values())
            {
                long now = System.currentTimeMillis();
                if (now < exitTime)
                {
                    long maxWaitTime = exitTime - now;
                    snapVlmDataInfo.daemon.awaitShutdown(maxWaitTime);
                }
            }
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
