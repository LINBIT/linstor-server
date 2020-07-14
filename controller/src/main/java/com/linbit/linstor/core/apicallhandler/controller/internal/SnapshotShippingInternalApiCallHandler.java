package com.linbit.linstor.core.apicallhandler.controller.internal;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiDataLoader;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotDeleteApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import reactor.core.publisher.Flux;

@Singleton
public class SnapshotShippingInternalApiCallHandler
{
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final ErrorReporter errorReporter;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final Provider<Peer> peerProvider;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final DynamicNumberPool snapshotShippingPortPool;
    private final CtrlSnapshotDeleteApiCallHandler snapshotDeleteApiCallHandler;

    @Inject
    public SnapshotShippingInternalApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        ErrorReporter errorReporterRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        Provider<Peer> peerProviderRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        @Named(NumberPoolModule.SNAPSHOPT_SHIPPING_PORT_POOL) DynamicNumberPool snapshotShippingPortPoolRef,
        CtrlSnapshotDeleteApiCallHandler snapshotDeleteApiCallHandlerRef
    )
    {
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        errorReporter = errorReporterRef;
        lockGuardFactory = lockGuardFactoryRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        peerProvider = peerProviderRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        snapshotShippingPortPool = snapshotShippingPortPoolRef;
        snapshotDeleteApiCallHandler = snapshotDeleteApiCallHandlerRef;
    }

    public boolean startShipping(Snapshot targetSnapRef)
    {
        boolean updateSatellite = false;
        SnapshotDefinition snapDfn = targetSnapRef.getSnapshotDefinition();
        try
        {
            String sourceNodeName = snapDfn.getProps(apiCtx).getProp(
                InternalApiConsts.KEY_SNAPSHOT_SHIPPING_SOURCE_NODE
            );
            Snapshot sourceSnap = snapDfn.getSnapshot(apiCtx, new NodeName(sourceNodeName));

            StateFlags<Snapshot.Flags> sourceSnapFlags = sourceSnap.getFlags();
            if (!sourceSnapFlags.isSet(apiCtx, Snapshot.Flags.SHIPPING_SOURCE_START))
            {
                sourceSnapFlags.enableFlags(apiCtx, Snapshot.Flags.SHIPPING_SOURCE_START);
                updateRscConnPropsPreStart(sourceSnap, targetSnapRef);
                updateSatellite = true;
            }
        }
        catch (InvalidKeyException | AccessDeniedException | InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (DatabaseException exc)
        {
            String errorMessage = String.format(
                "A database error occurred while trying to start snapshot shipping of " +
                    "resource: %s",
                targetSnapRef.getResourceName().displayValue
            );
            errorReporter.reportError(
                exc,
                apiCtx,
                peerProvider.get(),
                errorMessage
            );
        }
        return updateSatellite;
    }

    public Flux<ApiCallRc> shippingReceived(String rscNameRef, String snapNameRef, boolean successRef)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Finish received snapshot-shipping",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP).buildDeferred(),
                () -> shippingReceivedInTransaction(rscNameRef, snapNameRef, successRef)
            );
    }

    private Flux<ApiCallRc> shippingReceivedInTransaction(String rscNameRef, String snapNameRef, boolean successRef)
    {
        Peer stltPeer = peerProvider.get();
        SnapshotDefinition snapDfn = ctrlApiDataLoader.loadSnapshotDfn(rscNameRef, snapNameRef, true);
        Snapshot snapTarget = ctrlApiDataLoader.loadSnapshot(stltPeer.getNode(), snapDfn);
        Snapshot snapSource = getSnapshotShippingSource(snapDfn);

        updateRscConPropsAfterReceived(snapSource, snapTarget);

        Flux<ApiCallRc> flux;
        if (!successRef)
        {
            ctrlTransactionHelper.commit();

            // deletes the whole snapshotDfn
            flux = snapshotDeleteApiCallHandler.deleteSnapshot(rscNameRef, snapNameRef);
        }
        else
        {
            disableFlags(snapDfn, SnapshotDefinition.Flags.SHIPPING);
            enableFlags(snapDfn, SnapshotDefinition.Flags.SHIPPING_CLEANUP);

            copyLuksKeysIfNeeded(snapSource, snapTarget);

            ctrlTransactionHelper.commit();

            flux = ctrlSatelliteUpdateCaller.updateSatellites(
                snapDfn,
                CtrlSatelliteUpdateCaller.notConnectedWarn()
            ).transform(
                responses -> CtrlResponseUtils.combineResponses(
                    responses,
                    LinstorParsingUtils.asRscName(rscNameRef),
                    "Finishing shpipping of snapshot''" + snapNameRef + "'' of {1} on {0}"
                )
            ).concatWith(shippingMerged(rscNameRef, snapNameRef, successRef));
        }

        return flux;
    }

    public Flux<ApiCallRc> shippingMerged(String rscNameRef, String snapNameRef, boolean successRef)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Finish cleanup snapshot-shipping",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP).buildDeferred(),
                () -> shippingMergedInTransaction(rscNameRef, snapNameRef, successRef)
            );
    }

    private Flux<ApiCallRc> shippingMergedInTransaction(String rscNameRef, String snapNameRef, boolean successRef)
    {
        Peer stltPeer = peerProvider.get();
        SnapshotDefinition snapDfn = ctrlApiDataLoader.loadSnapshotDfn(rscNameRef, snapNameRef, true);
        Snapshot snapTarget = ctrlApiDataLoader.loadSnapshot(stltPeer.getNode(), snapDfn);
        Snapshot snapSource = getSnapshotShippingSource(snapDfn);

        updateRscConPropsAfterReceived(snapSource, snapTarget);

        snapshotShippingPortPool.deallocate(getPort(snapDfn));

        Flux<ApiCallRc> flux;
        if (!successRef)
        {
            ctrlTransactionHelper.commit();

            // deletes the whole snapshotDfn
            flux = snapshotDeleteApiCallHandler.deleteSnapshot(rscNameRef, snapNameRef);
        }
        else
        {
            disableFlags(snapDfn, SnapshotDefinition.Flags.SHIPPING_CLEANUP);
            enableFlags(snapDfn, SnapshotDefinition.Flags.SHIPPED);

            copyLuksKeysIfNeeded(snapSource, snapTarget);

            ctrlTransactionHelper.commit();

            flux = ctrlSatelliteUpdateCaller.updateSatellites(
                snapDfn,
                CtrlSatelliteUpdateCaller.notConnectedWarn()
            ).transform(
                responses -> CtrlResponseUtils.combineResponses(
                    responses,
                    LinstorParsingUtils.asRscName(rscNameRef),
                    "Finishing shpipping of snapshot''" + snapNameRef + "'' of {1} on {0}"
                )
            ).concatWith(snapshotDeleteApiCallHandler.cleanupOldShippedSnapshots(snapDfn.getResourceDefinition()));
        }

        return flux;
    }

    private Snapshot getSnapshotShippingSource(SnapshotDefinition snapDfnRef)
    {
        Snapshot snapShipSource;
        try
        {
            String sourceNodeName = snapDfnRef.getProps(apiCtx).getProp(
                InternalApiConsts.KEY_SNAPSHOT_SHIPPING_SOURCE_NODE
            );
            snapShipSource = snapDfnRef.getSnapshot(apiCtx, new NodeName(sourceNodeName));
        }
        catch (InvalidKeyException | AccessDeniedException | InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }
        return snapShipSource;
    }

    private void updateRscConnPropsPreStart(Snapshot snapSource, Snapshot snapTarget)
    {
        ResourceConnection rscConn = ctrlApiDataLoader.loadRscConn(
            snapSource.getResourceDefinition().getName(),
            snapSource.getNodeName(),
            snapTarget.getNodeName()
        );
        try
        {
            rscConn.setSnapshotShippingNameInProgress(snapSource.getSnapshotName());
        }
        catch (DatabaseException exc)
        {
            String errorMessage = String.format(
                "A database error occurred while finishing snapshot shipping of " +
                    "resource: %s",
                snapSource.getResourceName().displayValue
            );
            errorReporter.reportError(
                exc,
                apiCtx,
                peerProvider.get(),
                errorMessage
            );
        }
    }

    private void updateRscConPropsAfterReceived(Snapshot snapSource, Snapshot snapTarget)
    {
        ResourceConnection rscConn = ctrlApiDataLoader.loadRscConn(
            snapSource.getResourceDefinition().getName(),
            snapSource.getNodeName(),
            snapTarget.getNodeName()
        );
        try
        {
            Props rscConnProps = rscConn.getProps(apiCtx);
            rscConnProps.setProp(
                InternalApiConsts.KEY_SNAPSHOT_SHIPPING_NAME_PREV,
                snapSource.getSnapshotName().displayValue
            );
            rscConn.setSnapshotShippingNameInProgress(null);
        }
        catch (AccessDeniedException | InvalidKeyException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (DatabaseException exc)
        {
            String errorMessage = String.format(
                "A database error occurred while finishing snapshot shipping of " +
                    "resource: %s",
                snapSource.getResourceName().displayValue
            );
            errorReporter.reportError(
                exc,
                apiCtx,
                peerProvider.get(),
                errorMessage
            );
        }
    }

    private void enableFlags(SnapshotDefinition snapDfnRef, SnapshotDefinition.Flags... flags)
    {
        try
        {
            snapDfnRef.getFlags().enableFlags(apiCtx, flags);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        catch (DatabaseException exc)
        {
            String errorMessage = String.format(
                "A database error occurred while updating snapshot flags of " +
                    "resource: %s, snapshot: %s",
                snapDfnRef.getResourceName().displayValue,
                snapDfnRef.getName().displayValue
            );
            errorReporter.reportError(
                exc,
                apiCtx,
                peerProvider.get(),
                errorMessage
            );
        }
    }

    private void disableFlags(SnapshotDefinition snapDfnRef, SnapshotDefinition.Flags... flags)
    {
        try
        {
            snapDfnRef.getFlags().disableFlags(apiCtx, flags);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        catch (DatabaseException exc)
        {
            String errorMessage = String.format(
                "A database error occurred while updating snapshot flags of " +
                    "resource: %s, snapshot: %s",
                snapDfnRef.getResourceName().displayValue,
                snapDfnRef.getName().displayValue
            );
            errorReporter.reportError(
                exc,
                apiCtx,
                peerProvider.get(),
                errorMessage
            );
        }
    }

    private int getPort(SnapshotDefinition snapDfn)
    {
        try
        {
            return Integer.parseInt(snapDfn.getProps(apiCtx).getProp(InternalApiConsts.KEY_SNAPSHOT_SHIPPING_PORT));
        }
        catch (NumberFormatException | InvalidKeyException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    /**
     * copies the key not just from the snapshot-props, but also from the resource's layerdata
     *
     * @param snapSourceRef
     * @param snapTargetRef
     */
    private void copyLuksKeysIfNeeded(Snapshot snapSourceRef, Snapshot snapTargetRef)
    {
        try
        {
            Map<Pair<String, Integer>, byte[]> sourceKeys = new HashMap<>();

            Set<AbsRscLayerObject<Snapshot>> luksLayerObjSourceSet = LayerRscUtils
                .getRscDataByProvider(snapSourceRef.getLayerData(apiCtx), DeviceLayerKind.LUKS);
            if (!luksLayerObjSourceSet.isEmpty())
            {
                for (AbsRscLayerObject<Snapshot> luksLayerObj : luksLayerObjSourceSet)
                {
                    for (VlmProviderObject<Snapshot> luksLayerVlm : luksLayerObj.getVlmLayerObjects().values())
                    {
                        sourceKeys.put(
                            new Pair<>(
                                luksLayerVlm.getRscLayerObject().getSuffixedResourceName(),
                                luksLayerVlm.getVlmNr().value
                            ),
                            ((LuksVlmData<Snapshot>) luksLayerVlm).getEncryptedKey()
                        );
                    }
                }
                Set<AbsRscLayerObject<Resource>> luksLayerObjTargetSet = LayerRscUtils
                    .getRscDataByProvider(
                        snapTargetRef.getResourceDefinition().getResource(apiCtx, snapTargetRef.getNodeName()).getLayerData(apiCtx),
                        DeviceLayerKind.LUKS
                    );
                for (AbsRscLayerObject<Resource> luksLayerObj : luksLayerObjTargetSet)
                {
                    for (VlmProviderObject<Resource> luksLayerVlm : luksLayerObj.getVlmLayerObjects().values())
                    {
                        byte[] encryptedKey = sourceKeys.get(
                            new Pair<>(
                                luksLayerVlm.getRscLayerObject().getSuffixedResourceName(),
                                luksLayerVlm.getVlmNr().value
                            )
                        );
                        ((LuksVlmData<Resource>) luksLayerVlm).setEncryptedKey(encryptedKey);
                    }
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
    }

    private <T> Flux<T> deleteSnapshot(String rscNameRef, String snapNameRef)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Deleting received and merged snapshot",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP).buildDeferred(),
                () -> deleteSnapshotInTransaction(rscNameRef, snapNameRef)
            );
    }

    private <T> Flux<T> deleteSnapshotInTransaction(String rscNameRef, String snapNameRef)
    {
        Snapshot snapshot = ctrlApiDataLoader.loadSnapshot(
            peerProvider.get().getNode(),
            ctrlApiDataLoader.loadSnapshotDfn(rscNameRef, snapNameRef, true)
        );
        deleteSnapshotPrivileged(snapshot);
        ctrlTransactionHelper.commit();
        return Flux.empty();
    }

    private void deleteSnapshotPrivileged(Snapshot snapRef)
    {
        try
        {
            snapRef.delete(apiCtx);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (DatabaseException exc)
        {
            String errorMessage = String.format(
                "A database error occurred while trying delete snapshot after shipping shipping of " +
                    "resource: %s",
                snapRef.getResourceName().displayValue
            );
            errorReporter.reportError(
                exc,
                apiCtx,
                peerProvider.get(),
                errorMessage
            );
        }
    }
}
