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
import com.linbit.linstor.annotation.Nullable;
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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiConsumer;

@Deprecated(forRemoval = true)
@Singleton
public class SnapshotShippingService implements SystemService
{
    public static final ServiceName SERVICE_NAME;
    public static final String SERVICE_INFO = "SnapshotShippingService";

    private static final String CMD_FORMAT_RECEIVING =
        "set -o pipefail; " +
            "socat TCP-LISTEN:%s STDOUT | " +
            "zstd -d | " +
            // "pv -s 100m -bnr -i 0.1 | " +
            "%s";
    private static final String CMD_FORMAT_SENDING =
        "set -o pipefail; " +
            "%s | " +
            // "pv -s 100m -bnr -i 0.1 | " +
            "zstd | " +
            "socat STDIN TCP:%s:%s";

    private final AccessContext storDriverAccCtx;
    private final ExtCmdFactory extCmdFactory;
    private final ErrorReporter errorReporter;
    private final ControllerPeerConnector controllerPeerConnector;
    private final CtrlStltSerializer interComSerializer;

    private final Map<Snapshot, ShippingInfo> shippingInfoMap;
    private final Set<Snapshot> startedShippments;
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
        startedShippments = Collections.synchronizedSet(new TreeSet<>());
        threadGroup = new ThreadGroup("SnapshotShippingSerivceThreadGroup");
    }

    public void killAllShipping() throws StorageException
    {
        for (ShippingInfo shippingInfo : shippingInfoMap.values())
        {
            for (SnapVlmDataInfo snapVlmDataInfo : shippingInfo.snapVlmDataInfoMap.values())
            {
                snapVlmDataInfo.daemon.shutdown();
            }
        }
        shippingInfoMap.clear();
    }

    public void abort(AbsStorageVlmData<Snapshot> snapVlmData)
    {
        errorReporter.logDebug(
            "aborting snapshot shipping: %s",
            snapVlmData.getRscLayerObject().getAbsResource().toString()
        );
        Snapshot snap = snapVlmData.getRscLayerObject().getAbsResource();
        ShippingInfo info = shippingInfoMap.get(snap);
        if (info != null)
        {
            SnapVlmDataInfo snapVlmDataInfo = info.snapVlmDataInfoMap.get(snapVlmData);
            if (snapVlmDataInfo != null)
            {
                snapVlmDataInfo.daemon.shutdown();
            }
        }
        else
        {
            errorReporter.logDebug("  shippingInfo is null, nothing to shutdown");
        }
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
                "timeout",
                "0",
                "bash", "-c",
                String.format(CMD_FORMAT_RECEIVING, port, snapshotShippingReceivingCommandRef)
            },
            shippingDescr,
            (success, alreadyInUse) -> postShipping(
                success,
                alreadyInUse,
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
                "timeout",
                "0",
                "bash", "-c",
                String.format(
                    CMD_FORMAT_SENDING,
                    snapshotShippingSendingCommandRef,
                    targetNetIfRef.getAddress(storDriverAccCtx).getAddress(),
                    socatPortRef
                )
            },
            shippingDescr,
            (success, alreadyInUse) -> postShipping(
                success,
                alreadyInUse,
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
        BiConsumer<Boolean, Boolean> postAction,
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
        boolean alreadyInUseRef,
        AbsStorageVlmData<Snapshot> snapVlmData,
        @Nullable String internalApiName,
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
                else if (alreadyInUseRef)
                {
                    shippingInfo.vlmNrsWithBlockedPort.add(snapVlmData.getVlmNr().getValue());
                }
                if (shippingInfo.snapVlmDataFinishedShipping == shippingInfo.snapVlmDataInfoMap.size())
                {
                    if (updateCtrlRef)
                    {
                        boolean success = shippingInfo.snapVlmDataFinishedSuccessfully == shippingInfo.snapVlmDataFinishedShipping;
                        controllerPeerConnector.getControllerPeer().sendMessage(
                            interComSerializer.onewayBuilder(internalApiName)
                                .notifySnapshotShipped(snap, success, shippingInfo.vlmNrsWithBlockedPort).build(),
                            internalApiName
                        );
                    }

                    for (SnapVlmDataInfo snapVlmDataInfo : shippingInfo.snapVlmDataInfoMap.values())
                    {
                        snapVlmDataInfo.daemon.shutdown(); // just make sure that everything is already stopped
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
                            startedShippments.add(snap);
                        }
                        info.isStarted = true;
                    }
                }
            }
        }
    }

    private boolean alreadyStarted(AbsStorageVlmData<Snapshot> snapVlmDataRef)
    {
        return startedShippments.contains(snapVlmDataRef.getVolume().getAbsResource());
    }

    private void killIfRunning(String cmdToKill) throws StorageException
    {
        try
        {
            OutputData outputData = extCmdFactory.create().exec(
                "bash",
                "-c",
                "ps ax -o pid,command | grep '" + cmdToKill + "' | grep -v grep"
            );
            if (outputData.exitCode == 0) // != 0 means grep didnt find anything
            {
                String out = new String(outputData.stdoutData);
                String[] lines = out.split("\n");
                for (String line : lines)
                {
                    final String lineTrimmed = line.trim(); // ps prints a trailing space
                    String pid = lineTrimmed.substring(0, lineTrimmed.indexOf(" "));
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

    public void snapshotDeleted(Snapshot snap)
    {
        startedShippments.remove(snap);
    }

    private static class ShippingInfo
    {
        private boolean isStarted = false;
        private Map<AbsStorageVlmData<Snapshot>, SnapVlmDataInfo> snapVlmDataInfoMap = new HashMap<>();

        private int snapVlmDataFinishedShipping = 0;
        private int snapVlmDataFinishedSuccessfully = 0;
        private Set<Integer> vlmNrsWithBlockedPort = new HashSet<>();
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
