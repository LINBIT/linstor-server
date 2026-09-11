package com.linbit.linstor.api.protobuf.internal;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.CapacityInfoPojo;
import com.linbit.linstor.api.protobuf.ProtoDeserializationUtils;
import com.linbit.linstor.api.protobuf.ProtoUuidUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlMinIoSizeHelper;
import com.linbit.linstor.core.apicallhandler.controller.CtrlNodeApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.apicallhandler.controller.autohelper.AutoHelperContext;
import com.linbit.linstor.core.apicallhandler.controller.autohelper.CtrlRscAutoHelper;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlFullSyncResponseApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlFullSyncResponseApiCallHandler.FullSyncSuccessContext;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.controller.internal.StorPoolInternalCallHandler;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.storage.BlockSizeConsts;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.proto.common.StorPoolFreeSpaceOuterClass.StorPoolFreeSpace;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntFullSyncResponseOuterClass;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntFullSyncResponseOuterClass.MsgIntFullSyncResponse;
import com.linbit.linstor.storage.ProcCryptoEntry;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.utils.layer.LayerKindUtils;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.linstor.utils.layer.LayerVlmUtils;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;
import com.linbit.utils.MathUtils;

import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import reactor.core.publisher.Flux;

@ProtobufApiCall(
    name = InternalApiConsts.API_FULL_SYNC_RESPONSE,
    description = "Satellite's response to full sync data"
)
@Singleton
public class IntFullSyncResponse implements ApiCallReactive
{
    private final ErrorReporter errorReporter;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlSatelliteUpdateCaller stltUpdateCaller;
    private final StorPoolInternalCallHandler storPoolApiCallHandler;
    private final CtrlFullSyncResponseApiCallHandler ctrlFullSyncApiCallHandler;
    private final Provider<Peer> satelliteProvider;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlMinIoSizeHelper minIoSizeHelper;
    private final ResourceDefinitionRepository rscDfnRepo;
    private final CtrlRscAutoHelper autoHelper;

    @Inject
    public IntFullSyncResponse(
        ErrorReporter errorReporterRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlSatelliteUpdateCaller stltUpdateCallerRef,
        StorPoolInternalCallHandler storPoolApiCallHandlerRef,
        CtrlFullSyncResponseApiCallHandler ctrlFullSyncApiCallHandlerRef,
        Provider<Peer> satelliteProviderRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        ResourceDefinitionRepository rscDfnRepoRef,
        CtrlRscAutoHelper autoHelperRef,
        CtrlMinIoSizeHelper minIoSizeHelperRef
    )
    {
        errorReporter = errorReporterRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        stltUpdateCaller = stltUpdateCallerRef;
        storPoolApiCallHandler = storPoolApiCallHandlerRef;
        ctrlFullSyncApiCallHandler = ctrlFullSyncApiCallHandlerRef;
        satelliteProvider = satelliteProviderRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        rscDfnRepo = rscDfnRepoRef;
        autoHelper = autoHelperRef;
        minIoSizeHelper = minIoSizeHelperRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn) throws IOException
    {
        return processReactive(satelliteProvider.get(), msgDataIn);
    }

    public Flux<byte[]> processReactive(Peer satellitePeerRef, InputStream msgDataIn)
        throws IOException
    {
        ResponseContext context = CtrlNodeApiCallHandler.makeNodeContext(
            ApiOperation.makeCreateOperation(),
            satellitePeerRef.getNode().getName().displayValue
        );

        MsgIntFullSyncResponse msgIntFullSyncResponse = MsgIntFullSyncResponse.parseDelimitedFrom(msgDataIn);

        Flux<byte[]> flux;
        if (msgIntFullSyncResponse.getFullSyncResult() == MsgIntFullSyncResponseOuterClass.FullSyncResult.SUCCESS)
        {
            List<CapacityInfoPojo> capacityInfoPojoList = new ArrayList<>();
            for (StorPoolFreeSpace protoFreeSpace : msgIntFullSyncResponse.getFreeSpaceList())
            {
                capacityInfoPojoList.add(
                    new CapacityInfoPojo(
                        ProtoUuidUtils.deserialize(protoFreeSpace.getStorPoolUuid()),
                        protoFreeSpace.getStorPoolName(),
                        protoFreeSpace.getFreeCapacity(),
                        protoFreeSpace.getTotalCapacity(),
                        ProtoDeserializationUtils.parseApiCallRcList(protoFreeSpace.getErrorsList())
                    )
                );
            }

            try (LockGuard ignored = lockGuardFactory.build(LockType.WRITE, LockObj.NODES_MAP))
            {
                List<ProcCryptoEntry> cryptoEntries = msgIntFullSyncResponse.getCryptoEntriesList().stream()
                    .map(ce -> new ProcCryptoEntry(
                        ce.getName(),
                        ce.getDriver(),
                        ProcCryptoEntry.CryptoType.fromString(ce.getType()),
                        ce.getPriority()))
                    .collect(Collectors.toList());
                errorReporter.logTrace("CryptoEntries for %s: %s",
                    satellitePeerRef.getNode().getName(),
                    cryptoEntries.stream().map(ProcCryptoEntry::getName).collect(Collectors.toList()));
                satellitePeerRef.getNode().setCryptoEntries(cryptoEntries);
            }

            flux = scopeRunner.fluxInTransactionalScope(
                "Handle full sync api success",
                lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.NODES_MAP, LockObj.STOR_POOL_DFN_MAP),
                () -> updateCapacities(satellitePeerRef, capacityInfoPojoList)
            )
            .thenMany(
                scopeRunner.fluxInTransactionalScope(
                    "Apply minimum I/O size from storage pools to volumes",
                        lockGuardFactory.buildDeferred(
                            LockType.WRITE,
                            LockObj.NODES_MAP,
                            LockObj.RSC_DFN_MAP,
                            LockObj.STOR_POOL_DFN_MAP
                        ),
                    () -> updateVolumeMinIoSize(satellitePeerRef)
                )
                .thenMany(
                    ctrlFullSyncApiCallHandler.fullSyncSuccess(
                        new FullSyncSuccessContext(
                            satellitePeerRef,
                            msgIntFullSyncResponse.getNodePropsToSetMap(),
                            msgIntFullSyncResponse.getNodePropKeysToDeleteList(),
                            msgIntFullSyncResponse.getNodePropNamespacesToDeleteList()
                        ),
                        context
                    )
                    .thenMany(runAutoMagic(context))
                    .thenMany(Flux.empty())
                )
            );
        }
        else if (msgIntFullSyncResponse.getFullSyncResult() ==
            MsgIntFullSyncResponseOuterClass.FullSyncResult.FAIL_OUTDATED_FULL_SYNC_ID)
        {
            /*
             * Not a permanent failure: the satellite expects a FullSync based on a newer fullSyncId (a newer
             * authentication invalidated the id this FullSync was based on - "double reconnect" race). The
             * handler restarts the handshake if needed.
             */
            flux = ctrlFullSyncApiCallHandler.fullSyncOutdated(
                satellitePeerRef,
                msgIntFullSyncResponse.getFullSyncId()
            )
                .thenMany(Flux.empty());
        }
        else
        {
            ApiConsts.ConnectionStatus connectionStatus;
            switch (msgIntFullSyncResponse.getFullSyncResult())
            {
                case FAIL_MISSING_REQUIRED_EXT_TOOLS:
                    connectionStatus = ApiConsts.ConnectionStatus.MISSING_EXT_TOOLS;
                    break;
                case SUCCESS:
                case FAIL_OUTDATED_FULL_SYNC_ID:
                    throw new ImplementationError(
                        "unexpected enum type: " + msgIntFullSyncResponse.getFullSyncResult()
                    );
                case UNRECOGNIZED:
                case FAIL_UNKNOWN:
                default:
                    connectionStatus = ApiConsts.ConnectionStatus.FULL_SYNC_FAILED;
                    break;
            }
            flux = ctrlFullSyncApiCallHandler.fullSyncFailed(
                satellitePeerRef,
                connectionStatus
            )
                .thenMany(Flux.empty());
        }
        return flux;
    }


    private Flux<?> updateCapacities(Peer satellitePeerRef, List<CapacityInfoPojo> capacityInfoPojoList)
    {
        storPoolApiCallHandler.updateRealFreeSpace(satellitePeerRef, capacityInfoPojoList);
        return Flux.empty();
    }

    private Flux<?> updateVolumeMinIoSize(final Peer satellitePeer)
    {
        Flux<ApiCallRc> flux = Flux.empty();
        errorReporter.logDebug("ENTER updateVolumeMinIoSize");
        if (satellitePeer != null)
        {
            final Node satellite = satellitePeer.getNode();
            final NodeName satelliteName = satellite.getName();
            errorReporter.logDebug("updateVolumeMinIoSize: Peer \"%s\"", satelliteName.displayValue);
            final Iterator<Resource> localRscIter = satellite.iterateResources();
            final Set<ResourceDefinition> rscDfnsToUpdate = new HashSet<>();
            while (localRscIter.hasNext())
            {
                final Resource localRsc = localRscIter.next();
                final ResourceDefinition rscDfn = localRsc.getResourceDefinition();
                final ResourceName rscName = rscDfn.getName();

                final Iterator<VolumeDefinition> vlmDfnIter = rscDfn.iterateVolumeDfn();
                while (vlmDfnIter.hasNext())
                {
                    final VolumeDefinition vlmDfn = vlmDfnIter.next();
                    final Props vlmDfnProps = vlmDfn.getProps();

                    final boolean minIoSizeAuto = minIoSizeHelper.isAutoMinIoSize(vlmDfn);
                    if (minIoSizeAuto)
                    {
                        final VolumeNumber vlmNr = vlmDfn.getVolumeNumber();

                        errorReporter.logDebug(
                            "updateVolumeMinIoSize: Peer \"%s\", Resource \"%s\", Volume %d",
                            satelliteName.displayValue,
                            rscName.displayValue,
                            vlmNr.value
                        );

                        long minIoSize = BlockSizeConsts.DFLT_PHY_IO_SIZE;

                        final Iterator<Resource> rscIter = rscDfn.iterateResource();
                        while (rscIter.hasNext())
                        {
                            final Resource rsc = rscIter.next();
                            final boolean hasSpecialLayers = isResourceWithSpecialLayers(rsc);

                            final @Nullable Volume vlm = rsc.getVolume(vlmNr);
                            if (vlm != null)
                            {
                                final @Nullable StorPool dataStorPool = getDataStorPoolOfVolume(vlm);
                                if (dataStorPool != null)
                                {
                                    final long storPoolMinIoSize = getMinIoSizeForStorPool(dataStorPool);
                                    final long vlmMinIoSize = Math.max(
                                        hasSpecialLayers ?
                                            BlockSizeConsts.DFLT_SPECIAL_PHY_IO_SIZE :
                                            BlockSizeConsts.DFLT_PHY_IO_SIZE,
                                        storPoolMinIoSize
                                    );
                                    if (vlmMinIoSize > minIoSize)
                                    {
                                        minIoSize = vlmMinIoSize;
                                    }
                                }
                            }
                        }
                        minIoSize = MathUtils.bounds(
                            BlockSizeConsts.MIN_PHY_IO_SIZE, minIoSize, BlockSizeConsts.MAX_PHY_IO_SIZE
                        );
                        errorReporter.logDebug(
                            "updateVolumeMinIoSize: Peer \"%s\", Resource \"%s\", Volume %d: minIoSize = %d",
                            satelliteName.displayValue,
                            rscName.displayValue,
                            vlmNr.value,
                            minIoSize
                        );

                        boolean updateMinIoValue = true;
                        final @Nullable String storedPropValue = vlmDfnProps.getProp(
                            InternalApiConsts.KEY_DRBD_BLOCK_SIZE,
                            ApiConsts.NAMESPC_DRBD_DISK_OPTIONS
                        );
                        if (storedPropValue != null)
                        {
                            try
                            {
                                final long storedMinIoSize = Long.parseLong(storedPropValue);
                                if (storedMinIoSize == minIoSize)
                                {
                                    updateMinIoValue = false;
                                }
                            }
                            catch (NumberFormatException ignored)
                            {
                            }
                        }

                        if (updateMinIoValue)
                        {
                            try
                            {
                                final String propValue = Long.toString(minIoSize);
                                errorReporter.logDebug(
                                    "updateVolumeMinIoSize: Set property " +
                                    "namespace = \"%s\", key = \"%s\", value = \"%s\"",
                                    ApiConsts.NAMESPC_DRBD_DISK_OPTIONS,
                                    InternalApiConsts.KEY_DRBD_BLOCK_SIZE,
                                    propValue
                                );
                                vlmDfnProps.setProp(
                                    InternalApiConsts.KEY_DRBD_BLOCK_SIZE,
                                    propValue,
                                    ApiConsts.NAMESPC_DRBD_DISK_OPTIONS
                                );

                                Iterator<Resource> updRscIter = rscDfn.iterateResource();
                                while (updRscIter.hasNext())
                                {
                                    final Resource rsc = updRscIter.next();
                                    final Props rscProps = rsc.getProps();
                                    rscProps.setProp(
                                        InternalApiConsts.MIN_IO_SIZE_RESTART_DRBD,
                                        "true"
                                    );
                                }
                                rscDfnsToUpdate.add(rscDfn);
                            }
                            catch (InvalidKeyException ignored)
                            {
                                errorReporter.logDebug(
                                    "updateVolumeMinIoSize: Invalid property key %s/%s",
                                    ApiConsts.NAMESPC_DRBD_DISK_OPTIONS,
                                    InternalApiConsts.KEY_DRBD_BLOCK_SIZE
                                );
                            }
                            catch (InvalidValueException ignored)
                            {
                                errorReporter.logDebug(
                                    "updateVolumeMinIoSize: Invalid property value %s",
                                    Long.toString(minIoSize)
                                );
                            }
                            catch (DatabaseException dbExc)
                            {
                                final String errMsg = dbExc.getMessage();
                                errorReporter.logDebug(
                                    "updateVolumeMinIoSize: Database error: %s",
                                    errMsg == null ? "Database driver did not provide an error message" : errMsg
                                );
                            }
                        }
                    }

                }
            }
            ctrlTransactionHelper.commit();

            for (ResourceDefinition rscDfnToUpdate : rscDfnsToUpdate)
            {
                flux = flux.concatWith(
                    stltUpdateCaller.updateSatellites(rscDfnToUpdate, Flux.empty())
                        .transform(
                            updateResponses -> CtrlResponseUtils.combineResponses(
                                errorReporter,
                                updateResponses,
                                rscDfnToUpdate.getName(),
                                "Updated 'block-size' for DRBD on Resource definition {1} on {0}"
                        )
                    )
                );
            }
        }
        else
        {
            errorReporter.logDebug("updateVolumeMinIoSize: Peer object is a null pointer");
        }
        errorReporter.logDebug("EXIT updateVolumeMinIoSize");
        return flux;
    }

    private Flux<ApiCallRc> runAutoMagic(ResponseContext context)
    {
        return scopeRunner.fluxInTransactionalScope(
            "Run autohelper.manage after node fullsync",
            lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.NODES_MAP, LockObj.RSC_DFN_MAP),
            () -> runAutoMagicInTransaction(context)
        );
    }

    private Flux<ApiCallRc> runAutoMagicInTransaction(ResponseContext context)
    {
        ApiCallRcImpl apiCallRcImpl = new ApiCallRcImpl();
        List<Flux<ApiCallRc>> autoFluxes = new ArrayList<>();
        for (ResourceDefinition rscDfn : rscDfnRepo.getMapForView().values())
        {
            autoFluxes.add(autoHelper.manage(
                new AutoHelperContext(apiCallRcImpl, context, rscDfn)
            ).flux());
        }

        ctrlTransactionHelper.commit();
        return Flux.<ApiCallRc>just(apiCallRcImpl)
            .concatWith(Flux.merge(autoFluxes));
    }

    private boolean isResourceWithSpecialLayers(final Resource rsc)
    {
        List<DeviceLayerKind> layerStack = LayerRscUtils.getLayerStack(rsc);
        return LayerKindUtils.hasSpecialLayers(layerStack);
    }

    private long getMinIoSizeForStorPool(final StorPool storPoolObj)
    {
        long minIoSize = BlockSizeConsts.DFLT_PHY_IO_SIZE;
        final Props props = storPoolObj.getProps();
        final String minIoSizeStr = props.getProp(
            StorageConstants.BLK_DEV_MIN_IO_SIZE,
            StorageConstants.NAMESPACE_INTERNAL
        );
        if (minIoSizeStr != null)
        {
            try
            {
                minIoSize = Long.parseLong(minIoSizeStr);
            }
            catch (NumberFormatException ignored)
            {
            }
        }
        return minIoSize;
    }

    private @Nullable StorPool getDataStorPoolOfVolume(final Volume vlm)
    {
        return LayerVlmUtils.getStorPoolMap(vlm)
            .get(RscLayerSuffixes.SUFFIX_DATA);
    }
}
