package com.linbit.linstor.core.apicallhandler.satellite;

import com.linbit.ImplementationError;
import com.linbit.fsevent.FileSystemWatch;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageDriver;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.timer.CoreTimer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import javax.inject.Inject;
import javax.inject.Named;

import static java.util.stream.Collectors.toList;

public class StltApiCallHandlerUtils
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;

    private final FileSystemWatch fileSystemWatch;
    private final CoreTimer timer;
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock storPoolDfnMapLock;
    private final ControllerPeerConnector controllerPeerConnector;
    private final StltConfigAccessor stltCfgAccessor;

    @Inject
    public StltApiCallHandlerUtils(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        FileSystemWatch fileSystemWatchRef,
        CoreTimer timerRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef,
        StltConfigAccessor stltCfgAccessorRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        timer = timerRef;
        fileSystemWatch = fileSystemWatchRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        nodesMapLock = nodesMapLockRef;
        storPoolDfnMapLock = storPoolDfnMapLockRef;
        stltCfgAccessor = stltCfgAccessorRef;
    }

    public Map<StorPool, Long> getFreeSpace() throws StorageException
    {
        Map<StorPool, Long> freeSpaceMap = new HashMap<>();

        Lock nodesMapReadLock = nodesMapLock.readLock();
        Lock storPoolDfnMapReadLock = storPoolDfnMapLock.readLock();

        try
        {
            nodesMapReadLock.lock();
            storPoolDfnMapReadLock.lock();

            for (StorPool storPool : controllerPeerConnector.getLocalNode().streamStorPools(apiCtx).collect(toList()))
            {
                StorageDriver storageDriver = storPool.getDriver(
                    apiCtx,
                    errorReporter,
                    fileSystemWatch,
                    timer,
                    stltCfgAccessor
                );
                if (storageDriver != null)
                {
                    storPool.reconfigureStorageDriver(storageDriver);
                    Long freeSpace = storageDriver.getFreeSpace();
                    if (freeSpace != null)
                    {
                        freeSpaceMap.put(storPool, freeSpace);
                    }
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            errorReporter.reportError(new ImplementationError(exc));
        }
        finally
        {
            storPoolDfnMapReadLock.unlock();
            nodesMapReadLock.unlock();
        }

        return freeSpaceMap;
    }
}
