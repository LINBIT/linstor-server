package com.linbit.linstor.core.apicallhandler.controller.internal;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.pojo.CapacityInfoPojo;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiDataLoader;
import com.linbit.linstor.core.apicallhandler.controller.CtrlStorPoolApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuard;

import static com.linbit.linstor.core.apicallhandler.controller.helpers.StorPoolHelper.getStorPoolDescriptionInline;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;

public class StorPoolInternalCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final ResponseConverter responseConverter;
    private final CtrlStltSerializer ctrlStltSerializer;
    private final Provider<Peer> peer;
    private final Provider<AccessContext> peerAccCtx;

    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock storPoolDfnMapLock;

    @Inject
    public StorPoolInternalCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        ResponseConverter responseConverterRef,
        CtrlStltSerializer ctrlStltSerializerRef,
        Provider<Peer> peerRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        responseConverter = responseConverterRef;
        ctrlStltSerializer = ctrlStltSerializerRef;
        peer = peerRef;
        peerAccCtx = peerAccCtxRef;
        nodesMapLock = nodesMapLockRef;
        storPoolDfnMapLock = storPoolDfnMapLockRef;
    }

    public void handleStorPoolRequest(UUID storPoolUuid, String storPoolNameStr)
    {
        try (
            LockGuard ls = LockGuard.createLocked(
                nodesMapLock.readLock(),
                storPoolDfnMapLock.readLock(),
                peer.get().getSerializerLock().readLock()
            )
        )
        {
            StorPoolName storPoolName = new StorPoolName(storPoolNameStr);

            Peer currentPeer = peer.get();
            StorPool storPool = currentPeer.getNode().getStorPool(apiCtx, storPoolName);
            // TODO: check if the storPool has the same uuid as storPoolUuid
            if (storPool != null)
            {
                long fullSyncTimestamp = currentPeer.getFullSyncId();
                long updateId = currentPeer.getNextSerializerId();
                currentPeer.sendMessage(
                    ctrlStltSerializer
                        .onewayBuilder(InternalApiConsts.API_APPLY_STOR_POOL)
                        .storPool(storPool, fullSyncTimestamp, updateId)
                        .build()
                );
            }
            else
            {
                long fullSyncTimestamp = currentPeer.getFullSyncId();
                long updateId = currentPeer.getNextSerializerId();
                currentPeer.sendMessage(
                    ctrlStltSerializer
                        .onewayBuilder(InternalApiConsts.API_APPLY_STOR_POOL_DELETED)
                        .deletedStorPool(storPoolNameStr, fullSyncTimestamp, updateId)
                        .build()
                );
            }
        }
        catch (InvalidNameException invalidNameExc)
        {
            errorReporter.reportError(
                new ImplementationError(
                    "Satellite requested data for invalid storpool name '" + storPoolNameStr + "'.",
                    invalidNameExc
                )
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            errorReporter.reportError(
                new ImplementationError(
                    "Controller's api context has not enough privileges to gather requested storpool data.",
                    accDeniedExc
                )
            );
        }
    }

    private StorPool loadStorPool(String nodeNameStr, String storPoolNameStr, boolean failIfNull)
    {
        return ctrlApiDataLoader.loadStorPool(
            ctrlApiDataLoader.loadStorPoolDfn(storPoolNameStr, true),
            ctrlApiDataLoader.loadNode(nodeNameStr, true),
            failIfNull
        );
    }

    private void setCapacityInfo(StorPool storPool, long freeCapacity, long totalCapacity)
    {
        try
        {
            storPool.getFreeSpaceTracker().setCapacityInfo(peerAccCtx.get(), freeCapacity, totalCapacity);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "update free space of free space manager '" +
                    storPool.getFreeSpaceTracker().getName().displayValue +
                    "'",
                ApiConsts.FAIL_ACC_DENIED_FREE_SPACE_MGR
            );
        }
    }

    public void updateRealFreeSpace(List<CapacityInfoPojo> capacityInfoPojoList)
    {
        updateRealFreeSpace(peer.get(), capacityInfoPojoList);
    }

    public void updateRealFreeSpace(Peer peerRef, List<CapacityInfoPojo> capacityInfoPojoList)
    {
        try (LockGuard ls = LockGuard.createLocked(nodesMapLock.writeLock(), storPoolDfnMapLock.writeLock()))
        {
            Node node = peerRef.getNode();
            if (!node.isDeleted())
            {
                String nodeName = node.getName().displayValue;

                try
                {
                    for (CapacityInfoPojo capacityInfoPojo : capacityInfoPojoList)
                    {
                        ResponseContext context = CtrlStorPoolApiCallHandler.makeStorPoolContext(
                            ApiOperation.makeModifyOperation(),
                            nodeName,
                            capacityInfoPojo.getStorPoolName()
                        );

                        try
                        {
                            StorPool storPool = loadStorPool(nodeName, capacityInfoPojo.getStorPoolName(), true);
                            if (storPool.getUuid().equals(capacityInfoPojo.getStorPoolUuid()))
                            {
                                storPool.clearReports();
                                storPool.addReports(capacityInfoPojo.getErrors());
                                setCapacityInfo(
                                    storPool,
                                    capacityInfoPojo.getFreeCapacity(),
                                    capacityInfoPojo.getTotalCapacity()
                                );
                            }
                            else
                            {
                                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                                    ApiConsts.FAIL_UUID_STOR_POOL,
                                    "UUIDs mismatched when updating free space of " +
                                        getStorPoolDescriptionInline(storPool) + ".\nLocal UUID: " + storPool
                                            .getUuid() + ", UUID from pojo: " + capacityInfoPojo.getStorPoolUuid()
                                ));
                            }
                        }
                        catch (Exception | ImplementationError exc)
                        {
                            // Add context to exception
                            throw new ApiRcException(
                                responseConverter.exceptionToResponse(peerRef, context, exc), exc, true);
                        }
                    }

                    ctrlTransactionHelper.commit();
                }
                catch (ApiRcException exc)
                {
                    ApiCallRc apiCallRc = exc.getApiCallRc();
                    for (ApiCallRc.RcEntry entry : apiCallRc)
                    {
                        errorReporter.reportError(
                            exc.getCause() != null ? exc.getCause() : exc,
                            peerAccCtx.get(),
                            peerRef,
                            entry.getMessage()
                        );
                    }
                }
            }
        }
        // else: the node is deleted, thus if it still has any storpools left, those will
        // soon be deleted as well.
    }

    public void handleStorPoolApplied(
        String storPoolNameRef,
        boolean supportsSnapshotsRef,
        CapacityInfoPojo capacityInfoPojoRef
    )
    {
        Peer currentPeer = peer.get();
        StorPool storPool;
        try
        {
            storPool = currentPeer.getNode().getStorPool(apiCtx, new StorPoolName(storPoolNameRef));
            storPool.setSupportsSnapshot(apiCtx, supportsSnapshotsRef);
        }
        catch (AccessDeniedException | InvalidNameException | DatabaseException exc)
        {
            throw new ImplementationError(exc);
        }
        updateRealFreeSpace(currentPeer, Collections.singletonList(capacityInfoPojoRef));
    }
}
