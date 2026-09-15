package com.linbit.linstor.layer.resource;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.crypto.SecretGenerator;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.NodeIdAlloc;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.CoreModule.RemoteMap;
import com.linbit.linstor.core.apicallhandler.CtrlRscLayerDataMerger;
import com.linbit.linstor.core.apicallhandler.controller.CtrlNodeApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlPropsHelper;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.core.types.NodeId;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.AbsLayerHelperUtils;
import com.linbit.linstor.layer.LayerIgnoreReason;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.layer.LayerPayload.DrbdRscDfnPayload;
import com.linbit.linstor.layer.resource.CtrlRscLayerDataFactory.ChildResourceData;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.modularcrypto.ModularCryptoProvider;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmDfnData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject.TransportType;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscObject;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscObject.DrbdRscFlags;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerDataFactory;
import com.linbit.linstor.storage.utils.LayerUtils;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.linstor.utils.layer.LayerVlmUtils;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlVlmListApiCallHandler.getVlmDescriptionInline;

@Singleton
public class RscDrbdLayerHelper extends
    AbsRscLayerHelper<
    DrbdRscData<Resource>, DrbdVlmData<Resource>,
    DrbdRscDfnData<Resource>, DrbdVlmDfnData<Resource>
>
{
    private static final short DFLT_RESERVERD_PEER_SLOT_COUNT = (short) 1;

    private final CtrlPropsHelper ctrlPropsHelper;
    private final ResourceDefinitionRepository rscDfnMap;
    private final ModularCryptoProvider cryptoProvider;

    private final Provider<RscNvmeLayerHelper> nvmeHelperProvider;
    private final RemoteMap remoteMap;
    private final CtrlRscLayerDataMerger ctrlRscLayerDataMerger;


    @Inject
    RscDrbdLayerHelper(
        ErrorReporter errorReporter,
        ResourceDefinitionRepository rscDfnMapRef,
        LayerDataFactory layerDataFactory,
        CtrlPropsHelper ctrlPropsHelperRef,
        @Named(NumberPoolModule.LAYER_RSC_ID_POOL) DynamicNumberPool layerRscIdPool,
        Provider<CtrlRscLayerDataFactory> rscLayerDataFactory,
        Provider<RscNvmeLayerHelper> nvmeHelperProviderRef,
        ModularCryptoProvider cryptoProviderRef,
        RemoteMap remoteMapRef,
        CtrlRscLayerDataMerger ctrlRscLayerDataMergerRef
    )
    {
        super(
            errorReporter,
            layerDataFactory,
            layerRscIdPool,
            // DrdbRscData.class cannot directly be casted to Class<DrbdRscData<Resource>>. because java.
            // its type is Class<DrbdRscData> (without nested types), but that is not enough as the super constructor
            // wants a Class<RSC_PO>, where RSC_PO is DrbdRscData<Resource>.
            (Class<DrbdRscData<Resource>>) ((Object) DrbdRscData.class),
            DeviceLayerKind.DRBD,
            rscLayerDataFactory
        );
        rscDfnMap = rscDfnMapRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        nvmeHelperProvider = nvmeHelperProviderRef;
        cryptoProvider = cryptoProviderRef;
        remoteMap = remoteMapRef;
        ctrlRscLayerDataMerger = ctrlRscLayerDataMergerRef;
    }

    @Override
    protected DrbdRscDfnData<Resource> createRscDfnData(
        ResourceDefinition rscDfn,
        String rscNameSuffix,
        LayerPayload payload
    )
        throws DatabaseException, ValueOutOfRangeException
    {
        DrbdRscDfnPayload drbdRscDfnPayload = payload.drbdRscDfn;

        @Nullable Integer tcpPort = drbdRscDfnPayload.tcpPort;
        @Nullable TransportType transportType = drbdRscDfnPayload.transportType;
        @Nullable String secret = drbdRscDfnPayload.sharedSecret;
        @Nullable Short peerSlots = drbdRscDfnPayload.peerSlotsNewResource;
        @Nullable Short reserverdPeerSlotCount = drbdRscDfnPayload.reservedPeerSlotCount;
        @Nullable Integer alStripes = drbdRscDfnPayload.alStripes;
        @Nullable Long alStripeSize = drbdRscDfnPayload.alStripeSize;
        if (secret == null)
        {
            final SecretGenerator secretGen = cryptoProvider.createSecretGenerator();
            secret = secretGen.generateDrbdSharedSecret();
        }
        if (transportType == null)
        {
            transportType = TransportType.IP;
        }
        if (peerSlots == null)
        {
            peerSlots = getPeerSlotsForNewResource(rscDfn, payload);
        }
        if (alStripes == null)
        {
            alStripes = InternalApiConsts.DEFAULT_AL_STRIPES;
        }
        if (alStripeSize == null)
        {
            alStripeSize = InternalApiConsts.DEFAULT_AL_SIZE;
        }

        checkPeerSlotCount(peerSlots, reserverdPeerSlotCount, rscDfn);

        return layerDataFactory.createDrbdRscDfnData(
            rscDfn.getName(),
            null,
            rscNameSuffix,
            peerSlots,
            alStripes,
            alStripeSize,
            tcpPort,
            transportType,
            secret
        );
    }

    @Override
    protected void mergeRscDfnData(DrbdRscDfnData<Resource> drbdRscDfnData, LayerPayload payload)
        throws DatabaseException, ValueOutOfRangeException
    {
        DrbdRscDfnPayload drbdRscDfnPayload = payload.drbdRscDfn;

        if (drbdRscDfnPayload.tcpPort != null)
        {
            drbdRscDfnData.setPort(drbdRscDfnPayload.tcpPort);
        }
        if (drbdRscDfnPayload.transportType != null)
        {
            drbdRscDfnData.setTransportType(drbdRscDfnPayload.transportType);
        }
        if (drbdRscDfnPayload.sharedSecret != null)
        {
            drbdRscDfnData.setSecret(drbdRscDfnPayload.sharedSecret);
        }
        if (drbdRscDfnPayload.peerSlotsNewResource != null)
        {
            ResourceDefinition rscDfn = rscDfnMap.get(drbdRscDfnData.getResourceName());
            checkPeerSlotCount(drbdRscDfnPayload.peerSlotsNewResource, drbdRscDfnPayload.reservedPeerSlotCount, rscDfn);
            drbdRscDfnData.setPeerSlots(drbdRscDfnPayload.peerSlotsNewResource);
        }
    }

    @Override
    protected DrbdVlmDfnData<Resource> createVlmDfnData(
        VolumeDefinition vlmDfn,
        String rscNameSuffix,
        LayerPayload payload
    )
        throws DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
            ValueInUseException, LinStorException
    {
        DrbdRscDfnData<Resource> drbdRscDfnData = ensureResourceDefinitionExists(
            vlmDfn.getResourceDefinition(),
            rscNameSuffix,
            payload
        );
        return layerDataFactory.createDrbdVlmDfnData(
            vlmDfn,
            vlmDfn.getResourceDefinition().getName(),
            null,
            rscNameSuffix,
            vlmDfn.getVolumeNumber(),
            payload.drbdVlmDfn.minorNr,
            drbdRscDfnData
        );
    }

    @Override
    protected void mergeVlmDfnData(DrbdVlmDfnData<Resource> vlmDfnDataRef, LayerPayload payloadRef)
    {
        // minor number cannot be changed

        // no-op
    }

    @Override
    protected DrbdRscData<Resource> createRscData(
        Resource rscRef,
        LayerPayload payloadRef,
        String rscNameSuffixRef,
        AbsRscLayerObject<Resource> parentObjectRef,
        List<DeviceLayerKind> layerListRef
    )
        throws DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException, ImplementationError, InvalidNameException, LinStorException
    {
        ResourceDefinition rscDfn = rscRef.getResourceDefinition();
        @Nullable DrbdRscDfnData<Resource> drbdRscDfnData = ensureResourceDefinitionExists(
            rscDfn,
            rscNameSuffixRef,
            payloadRef
        );

        if (drbdRscDfnData == null)
        {
            throw new ImplementationError("DrbdRscDfnData was unexpectedly null. " + rscDfn + " " + rscNameSuffixRef);
        }

        NodeId nodeId = getNodeId(rscRef, payloadRef, rscNameSuffixRef, layerListRef, drbdRscDfnData, null);

        long initFlags = payloadRef.drbdRsc.rscFlags == null ? 0 : payloadRef.drbdRsc.rscFlags;
        @Nullable Short peerSlotsForNewRsc = null;
        // some flags that might already be set on resource level should be copied to DrbdRscData level
        if (isResourceDiskless(rscRef))
        {
            initFlags |= DrbdRscObject.DrbdRscFlags.DISKLESS.flagValue;
        }
        else
        {
            peerSlotsForNewRsc = drbdRscDfnData.getPeerSlots();
            checkIfPeersHaveEnoughPeerSlots(drbdRscDfnData);
            checkPeerSlotCount(peerSlotsForNewRsc, DFLT_RESERVERD_PEER_SLOT_COUNT, rscDfn);
        }

        int layerRscId = layerRscIdPool.autoAllocate();
        @Nullable Integer oldLayerRscId = payloadRef.drbdRsc.replacingOldLayerRscId;
        if (oldLayerRscId != null)
        {
            ctrlRscLayerDataMerger.replacedLayerRscId(oldLayerRscId, layerRscId);
        }

        return layerDataFactory.createDrbdRscData(
            layerRscId,
            rscRef,
            rscNameSuffixRef,
            parentObjectRef,
            drbdRscDfnData,
            nodeId,
            TcpPortNumber.parse(payloadRef.drbdRsc.tcpPorts),
            payloadRef.drbdRsc.portCount,
            null,
            null,
            null,
            initFlags
        );
    }


    private boolean isResourceDiskless(Resource rscRef)
    {
        return rscRef.getStateFlags().isSomeSet(
            Resource.Flags.DISKLESS,
            Resource.Flags.DRBD_DISKLESS,
            Resource.Flags.TIE_BREAKER
        );
    }

    private NodeId getNodeId(
        Resource rscRef,
        LayerPayload payloadRef,
        String rscNameSuffixRef,
        List<DeviceLayerKind> layerListRef,
        DrbdRscDfnData<Resource> drbdRscDfnData,
        @Nullable NodeId oldNodeIdRef
    )
        throws InvalidNameException, DatabaseException, ImplementationError,
        ExhaustedPoolException, ValueOutOfRangeException
    {
        NodeId nodeId = oldNodeIdRef;

        boolean isNvmeBelow = layerListRef.contains(DeviceLayerKind.NVME);
        boolean isNvmeInitiator = rscRef.getStateFlags().isSet(Resource.Flags.NVME_INITIATOR);
        boolean isEbsInitiator = rscRef.getStateFlags().isSet(Resource.Flags.EBS_INITIATOR);

        Set<StorPool> allStorPools = layerDataHelperProvider.get()
            .getAllNeededStorPools(rscRef, payloadRef, layerListRef);
        Set<SharedStorPoolName> sharedStorPoolNames = allStorPools.stream()
            .map(StorPool::getSharedStorPoolName)
            .collect(Collectors.toSet());

        boolean isStoragePoolShared = areAllShared(allStorPools);

        if ((isNvmeBelow && isNvmeInitiator) || isEbsInitiator)
        {
            // we need to find our target resource and copy the node-id from that target-resource
            final Resource targetRsc;
            if (isNvmeInitiator)
            {
                targetRsc = nvmeHelperProvider.get().getTarget(rscRef);
            }
            else if (isEbsInitiator)
            {
                Set<String> availabilityZones = new HashSet<>();
                for (StorPool sp : allStorPools)
                {
                    availabilityZones.add(
                        RscStorageLayerHelper.getAvailabilityZone(remoteMap, sp, ctrlPropsHelper.getCtrlPropsForView())
                    );
                }
                if (availabilityZones.size() != 1)
                {
                    throw new ImplementationError(
                        "Unexpected count of availability zone. 1 entry expected. Found: " + availabilityZones +
                            " in storage pools: " + allStorPools
                    );
                }

                targetRsc = RscStorageLayerHelper.findTargetEbsResource(
                    remoteMap,
                    rscRef.getResourceDefinition(),
                    availabilityZones.iterator().next(),
                    rscRef.getNode().getName().displayValue,
                    ctrlPropsHelper.getCtrlPropsForView()
                );
            }
            else
            {
                throw new ImplementationError("Unknown target type");
            }
            if (targetRsc != null)
            {
                AbsRscLayerObject<Resource> rootLayerData = targetRsc.getLayerData();
                List<AbsRscLayerObject<Resource>> targetDrbdChildren = LayerUtils
                    .getChildLayerDataByKind(rootLayerData, DeviceLayerKind.DRBD);
                for (AbsRscLayerObject<Resource> targetRscData : targetDrbdChildren)
                {
                    if (targetRscData.getResourceNameSuffix().equals(rscNameSuffixRef))
                    {
                        nodeId = ((DrbdRscData<Resource>) targetRscData).getNodeId();
                        break;
                    }
                }
            }
            if (nodeId == null)
            {
                final long errorId;
                if (isNvmeInitiator)
                {
                    errorId = ApiConsts.FAIL_MISSING_NVME_TARGET;
                }
                else if (isEbsInitiator)
                {
                    errorId = ApiConsts.FAIL_MISSING_EBS_TARGET;
                }
                else
                {
                    throw new ImplementationError("Missing target resource of unknown kind");
                }
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        errorId,
                        "Failed to find target resource "
                    )
                );
            }
        }
        else if (isStoragePoolShared)
        {
            for (DrbdRscData<Resource> peerData : drbdRscDfnData.getDrbdRscDataList())
            {
                Set<StorPool> peerStorPools = LayerVlmUtils.getStorPools(peerData.getAbsResource());
                Set<SharedStorPoolName> peerSharedStorPoolNames = peerStorPools.stream()
                    .map(StorPool::getSharedStorPoolName)
                    .collect(Collectors.toSet());

                peerSharedStorPoolNames.retainAll(sharedStorPoolNames);
                if (!peerSharedStorPoolNames.isEmpty())
                {
                    nodeId = peerData.getNodeId();
                    break;
                }
            }
            // if not found, keep nodeId = null, next if will get a fresh nodeId for this drbdRscData
        }
        if (nodeId == null)
        {
            nodeId = getNodeId(payloadRef.drbdRsc.nodeId, drbdRscDfnData);
        }
        return nodeId;
    }

    @Override
    protected void mergeRscData(DrbdRscData<Resource> drbdRscData, LayerPayload payloadRef)
        throws DatabaseException, InvalidNameException, ImplementationError,
        ExhaustedPoolException, ValueOutOfRangeException, ValueInUseException
    {
        Resource rsc = drbdRscData.getAbsResource();

        if (isResourceDiskless(rsc))
        {
            drbdRscData.getFlags().enableFlags(DrbdRscFlags.DISKLESS);
        }
        else
        {
            drbdRscData.getFlags().disableFlags(DrbdRscFlags.DISKLESS);
        }

        NodeId oldNodeId = drbdRscData.getNodeId();
        NodeId newNodeId = getNodeId(
            rsc,
            payloadRef,
            drbdRscData.getResourceNameSuffix(),
            LayerRscUtils.getLayerStack(rsc),
            drbdRscData.getRscDfnLayerObject(),
            payloadRef.drbdRsc.needsNewNodeId ? null : oldNodeId
        );
        drbdRscData.setNodeId(newNodeId);

        if (payloadRef.drbdRsc.tcpPorts != null)
        {
            drbdRscData.setPorts(TcpPortNumber.parse(payloadRef.drbdRsc.tcpPorts));
        }
    }

    @Override
    protected boolean needsChildVlm(AbsRscLayerObject<Resource> childRscDataRef, Volume vlmRef)
        throws InvalidKeyException
    {
        boolean needsChild;
        AbsRscLayerObject<Resource> drbdRscData = childRscDataRef.getParent();
        if (childRscDataRef.getResourceNameSuffix().equals(drbdRscData.getResourceNameSuffix()))
        {
            // data child
            needsChild = true;
        }
        else
        {
            needsChild = isUsingExternalMetaData(vlmRef);
        }
        return needsChild;
    }

    private boolean isDrbdDiskless(AbsRscLayerObject<Resource> childRscDataRef)
    {
        StateFlags<Flags> rscFlags = childRscDataRef.getAbsResource().getStateFlags();
        return rscFlags.isSet(Resource.Flags.DRBD_DISKLESS) &&
            !rscFlags.isSomeSet(
                Resource.Flags.DISK_ADD_REQUESTED,
                Resource.Flags.DISK_ADDING,
                Resource.Flags.DISK_REMOVING,
                Resource.Flags.DISK_REMOVE_REQUESTED
            );
    }

    @Override
    protected Set<StorPool> getNeededStoragePools(
        Resource rsc,
        VolumeDefinition vlmDfn,
        LayerPayload payloadRef,
        List<DeviceLayerKind> layerListRef
    )
        throws InvalidNameException
    {
        Set<StorPool> storPools = new HashSet<>();
        Set<AbsRscLayerObject<Resource>> drbdRscDataSet = LayerRscUtils.getRscDataByLayer(
            rsc.getLayerData(),
            DeviceLayerKind.DRBD
        );
        for (AbsRscLayerObject<Resource> drbdRscData : drbdRscDataSet)
        {
            if (needsMetaData((DrbdRscData<Resource>) drbdRscData, layerListRef))
            {
                StorPool metaStorPool = getMetaStorPool(rsc, vlmDfn);
                if (metaStorPool != null)
                {
                    /*
                     * needsMetaData returns true if any of our vlms need external metadata
                     * but that does not mean that all vlms do.
                     */
                    storPools.add(metaStorPool);
                }
            }
        }
        return storPools;
    }

    @Override
    protected DrbdVlmData<Resource> createVlmLayerData(
        DrbdRscData<Resource> drbdRscData,
        Volume vlm,
        LayerPayload payload,
        List<DeviceLayerKind> layerListRef
    )
        throws DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
            ValueInUseException, LinStorException, InvalidKeyException
    {
        DrbdVlmDfnData<Resource> drbdVlmDfnData = ensureVolumeDefinitionExists(
            vlm.getVolumeDefinition(),
            drbdRscData.getResourceNameSuffix(),
            payload
        );
        StorPool extMetaStorPool = null;
        if (needsMetaData(drbdRscData, layerListRef))
        {
            extMetaStorPool = getExternalMetaDiskStorPool(vlm);
        }
        DrbdVlmData<Resource> drbdVlmData = layerDataFactory.createDrbdVlmData(
            vlm,
            extMetaStorPool,
            drbdRscData,
            drbdVlmDfnData
        );

        Resource rsc = vlm.getAbsResource();
        if (rsc.isDrbdDiskfulFlagsCheck(layerListRef))
        {
            VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
            Props vlmDfnProps = vlmDfn.getProps();
            @Nullable String winner = vlmDfnProps.getProp(InternalApiConsts.KEY_LINSTOR_DRBD_INITIAL_UPTODATE_ON);
            if (winner == null)
            {
                Optional<Resource> primaryRsc = rsc.getResourceDefinition().anyResourceInUse();
                if (primaryRsc.isPresent())
                {
                    winner = primaryRsc.get().getNode().getName().value;
                }
                else
                {
                    winner = rsc.getNode().getName().value;
                }

                try
                {
                    vlmDfnProps.setProp(
                        InternalApiConsts.KEY_LINSTOR_DRBD_INITIAL_UPTODATE_ON,
                        winner
                    );
                }
                catch (InvalidKeyException | InvalidValueException exc)
                {
                    throw new ImplementationError(exc);
                }
            }
        }

        return drbdVlmData;
    }

    private @Nullable StorPool getExternalMetaDiskStorPool(Volume vlm)
        throws InvalidKeyException
    {
        String extMetaStorPoolNameStr = getExtMetaDataStorPoolName(vlm);
        StorPool extMetaStorPool = null;
        if (isExternalMetaDataPool(extMetaStorPoolNameStr))
        {
            try
            {
                extMetaStorPool = vlm.getAbsResource().getNode().getStorPool(
                    new StorPoolName(extMetaStorPoolNameStr)
                );

                if (extMetaStorPool == null)
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_NOT_FOUND_STOR_POOL,
                            "The " + getVlmDescriptionInline(vlm) + " specified '" + extMetaStorPoolNameStr +
                                "' as the storage pool for external meta-data. Node " +
                                vlm.getAbsResource().getNode().getName() + " does not have a storage pool" +
                                " with that name"
                        )
                    );
                }
            }
            catch (InvalidNameException exc)
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_INVLD_STOR_POOL_NAME,
                        "The " + getVlmDescriptionInline(vlm) + " specified '" + extMetaStorPoolNameStr +
                        "' as the storage pool for external meta-data. That name is invalid."
                    ),
                    exc
                );
            }
        }
        return extMetaStorPool;
    }

    @Override
    protected void mergeVlmData(
        DrbdVlmData<Resource> drbdVlmData,
        Volume vlmRef,
        LayerPayload payloadRef,
        List<DeviceLayerKind> layerListRef
    )
    {
        // no-op
    }

    private boolean isUsingExternalMetaData(Volume vlmRef)
        throws InvalidKeyException
    {
        return isExternalMetaDataPool(getExtMetaDataStorPoolName(vlmRef));
    }

    private String getExtMetaDataStorPoolName(Volume vlmRef) throws InvalidKeyException
    {
        VolumeDefinition vlmDfn = vlmRef.getVolumeDefinition();
        ResourceDefinition rscDfn = vlmRef.getResourceDefinition();
        ResourceGroup rscGrp = rscDfn.getResourceGroup();
        Resource rsc = vlmRef.getAbsResource();
        return new PriorityProps(
            vlmDfn.getProps(),
            rscGrp.getVolumeGroupProps(vlmDfn.getVolumeNumber()),
            rsc.getProps(),
            rscDfn.getProps(),
            rscGrp.getProps(),
            rsc.getNode().getProps()
        ).getProp(
            ApiConsts.KEY_STOR_POOL_DRBD_META_NAME
        );
    }

    private boolean isExternalMetaDataPool(String metaPoolStr)
    {
        return metaPoolStr != null &&
            !metaPoolStr.trim().isEmpty() &&
            !metaPoolStr.equalsIgnoreCase(ApiConsts.VAL_STOR_POOL_DRBD_META_INTERNAL);
    }

    @Override
    protected List<ChildResourceData> getChildRsc(
        DrbdRscData<Resource> rscDataRef,
        List<DeviceLayerKind> layerListRef
    )
        throws InvalidKeyException
    {
        List<ChildResourceData> ret = new ArrayList<>();

        if (isDrbdDiskless(rscDataRef))
        {
            ret.add(
                new ChildResourceData(
                    RscLayerSuffixes.SUFFIX_DATA,
                    Collections.singleton(LayerIgnoreReason.DRBD_DISKLESS),
                    DeviceLayerKind.STORAGE
                )
            );
        }
        else
        {
            ret.add(new ChildResourceData(RscLayerSuffixes.SUFFIX_DATA, null));
        }

        if (needsMetaData(rscDataRef, layerListRef))
        {
            ret.add(
                new ChildResourceData(
                    RscLayerSuffixes.SUFFIX_DRBD_META,
                    null,
                    DeviceLayerKind.STORAGE
                )
            );
        }

        return ret;
    }

    private boolean needsMetaData(
        DrbdRscData<Resource> drbdRscDataRef,
        List<DeviceLayerKind> layerListRef
    )
    {
        boolean ret;

        if (loadingFromDatabase)
        {
            /*
             * ignore properties when loading from DB.
             * in this case we already have loaded all resource-layer-trees, therefore we can simply
             * lookup if we have a direct meta-child
             */
            ret = drbdRscDataRef.getChildBySuffix(RscLayerSuffixes.SUFFIX_DRBD_META) != null;
        }
        else
        {
            boolean allVlmsUseInternalMetaData = true;
            Resource rsc = drbdRscDataRef.getAbsResource();
            ResourceDefinition rscDfn = rsc.getResourceDefinition();

            Iterator<VolumeDefinition> iterateVolumeDfn = rscDfn.iterateVolumeDfn();
            ReadOnlyProps rscProps = rsc.getProps();
            ReadOnlyProps rscDfnProps = rscDfn.getProps();
            ReadOnlyProps rscGrpProps = rscDfn.getResourceGroup().getProps();
            ReadOnlyProps nodeProps = rsc.getNode().getProps();

            while (iterateVolumeDfn.hasNext())
            {
                String metaPool = new PriorityProps(
                    iterateVolumeDfn.next().getProps(),
                    rscProps,
                    rscDfnProps,
                    rscGrpProps,
                    nodeProps
                ).getProp(ApiConsts.KEY_STOR_POOL_DRBD_META_NAME);
                if (isExternalMetaDataPool(metaPool))
                {
                    allVlmsUseInternalMetaData = false;
                    break;
                }
            }

            /*
             * We cannot use ResourceDataUtils.isDrbdResource here since that method is based on the not yet created
             * drbdRscData. As that object is yet to be created, the .isDrbdResource will (at this stage) always
             * return NO_DRBD (or throws a NullPointerException).
             */
            StateFlags<Flags> rscFlags = rsc.getStateFlags();
            // skip ignoreReasonCheck
            boolean needsMetaDisk = !rscFlags.isSomeSet(
                Resource.Flags.DELETE,
                Resource.Flags.INACTIVATING,
                Resource.Flags.INACTIVE,
                Resource.Flags.INACTIVE_PERMANENTLY,
                Resource.Flags.DRBD_DISKLESS
            );
            boolean isNvmeBelow = layerListRef.contains(DeviceLayerKind.NVME);
            boolean isNvmeInitiator = rscFlags.isSet(Resource.Flags.NVME_INITIATOR);

            needsMetaDisk &= (!isNvmeBelow || isNvmeInitiator);
            ret = !allVlmsUseInternalMetaData && needsMetaDisk;
        }
        return ret;
    }

    @Override
    public StorPool getStorPool(Volume vlmRef, AbsRscLayerObject<Resource> childRef)
        throws InvalidKeyException, InvalidNameException
    {
        StorPool metaStorPool = null;
        if (childRef.getSuffixedResourceName().contains(RscLayerSuffixes.SUFFIX_DRBD_META))
        {
            DrbdVlmData<Resource> drbdVlmData = (DrbdVlmData<Resource>) childRef.getParent()
                .getVlmProviderObject(vlmRef.getVolumeDefinition().getVolumeNumber());
            metaStorPool = drbdVlmData.getExternalMetaDataStorPool();
        }
        return metaStorPool;
    }

    @Override
    protected void resetStoragePools(AbsRscLayerObject<Resource> rscDataRef)
        throws DatabaseException, InvalidKeyException
    {
        DrbdRscData<Resource> drbdRscData = (DrbdRscData<Resource>) rscDataRef;
        for (DrbdVlmData<Resource> drbdVlmData : drbdRscData.getVlmLayerObjects().values())
        {
            drbdVlmData.setExternalMetaDataStorPool(
                getExternalMetaDiskStorPool((Volume) drbdVlmData.getVolume())
            );
        }
    }

    @Override
    protected boolean recalculateVolatilePropertiesImpl(
        DrbdRscData<Resource> rscDataRef,
        List<DeviceLayerKind> layerListRef,
        LayerPayload payloadRef
    )
        throws DatabaseException
    {
        boolean changed = false;
        if (isDrbdDiskless(rscDataRef))
        {
            changed = addIgnoreReason(rscDataRef, LayerIgnoreReason.DRBD_DISKLESS, false, true, true);
        }
        if (rscDataRef.isSkipDiskEnabled(ctrlPropsHelper.getStltPropsForView()))
        {
            changed |= addIgnoreReason(rscDataRef, LayerIgnoreReason.DRBD_SKIP_DISK, false, true, true);
        }
        return changed;
    }

    public @Nullable StorPool getMetaStorPool(Volume vlmRef)
        throws InvalidNameException
    {
        return getMetaStorPool(vlmRef.getAbsResource(), vlmRef.getVolumeDefinition());
    }

    public @Nullable StorPool getMetaStorPool(Resource rsc, VolumeDefinition vlmDfn)
        throws InvalidNameException
    {
        return getMetaStorPool(rsc, getPrioProps(rsc, vlmDfn));
    }

    private @Nullable StorPool getMetaStorPool(
        Resource rsc,
        PriorityProps prioProps
    )
        throws InvalidNameException
    {
        StorPool metaStorPool = null;
        String metaStorPoolStr = prioProps.getProp(ApiConsts.KEY_STOR_POOL_DRBD_META_NAME);
        if (
            isExternalMetaDataPool(metaStorPoolStr) &&
                !rsc.getStateFlags().isSet(Resource.Flags.DRBD_DISKLESS)
        )
        {
            metaStorPool = rsc.getNode().getStorPool(new StorPoolName(metaStorPoolStr));
            if (metaStorPool == null)
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_NOT_FOUND_STOR_POOL,
                        "Configured (meta) storage pool '" + metaStorPoolStr + "' not found on " +
                            CtrlNodeApiCallHandler.getNodeDescriptionInline(rsc.getNode())
                    )
                );
            }
        }
        return metaStorPool;
    }

    private PriorityProps getPrioProps(Resource rsc, VolumeDefinition vlmDfn)
    {
        ResourceGroup rscGrp = vlmDfn.getResourceDefinition().getResourceGroup();
        Node node = rsc.getNode();
        return new PriorityProps(
            vlmDfn.getProps(),
            rscGrp.getVolumeGroupProps(vlmDfn.getVolumeNumber()),
            rsc.getProps(),
            vlmDfn.getResourceDefinition().getProps(),
            rscGrp.getProps(),
            node.getProps()
        );
    }

    @Override
    protected boolean isExpectedToProvideDevice(DrbdRscData<Resource> drbdRscData)
    {
        return !drbdRscData.hasAnyPreventExecutionIgnoreReason();
    }

    @Override
    protected <RSC extends AbsResource<RSC>> DrbdRscDfnData<Resource> restoreRscDfnData(
        ResourceDefinition rscDfnRef,
        AbsRscLayerObject<RSC> fromSnapDataRef
    ) throws DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        String resourceNameSuffix = fromSnapDataRef.getResourceNameSuffix();
        DrbdRscDfnData<RSC> dfnData;
        RSC absRsc = fromSnapDataRef.getAbsResource();
        if (absRsc instanceof Snapshot snap)
        {
            dfnData = snap.getSnapshotDefinition().getLayerData(
                DeviceLayerKind.DRBD,
                resourceNameSuffix
            );
        }
        else if (absRsc instanceof Resource)
        {
            dfnData = absRsc.getResourceDefinition().getLayerData(
                DeviceLayerKind.DRBD,
                resourceNameSuffix
            );
        }
        else
        {
            throw new ImplementationError("Unexpected AbsRsc Type");
        }

        final SecretGenerator secretGen = cryptoProvider.createSecretGenerator();
        return layerDataFactory.createDrbdRscDfnData(
            rscDfnRef.getName(),
            null,
            resourceNameSuffix,
            dfnData.getPeerSlots(),
            dfnData.getAlStripes(),
            dfnData.getAlStripeSize(),
            null,
            dfnData.getTransportType(),
            secretGen.generateDrbdSharedSecret()
        );
    }

    @Override
    protected <RSC extends AbsResource<RSC>> DrbdRscData<Resource> restoreRscData(
        Resource rscRef,
        AbsRscLayerObject<RSC> fromAbsRscDataRef,
        AbsRscLayerObject<Resource> rscParentRef
    )
        throws DatabaseException, ExhaustedPoolException, ValueOutOfRangeException,
        ValueInUseException
    {
        DrbdRscData<Snapshot> drbdSnapData = (DrbdRscData<Snapshot>) fromAbsRscDataRef;
        String resourceNameSuffix = drbdSnapData.getResourceNameSuffix();
        DrbdRscDfnData<Resource> drbdRscDfnData = rscRef.getResourceDefinition().getLayerData(
            DeviceLayerKind.DRBD,
            resourceNameSuffix
        );

        return layerDataFactory.createDrbdRscData(
            layerRscIdPool.autoAllocate(),
            rscRef,
            resourceNameSuffix,
            rscParentRef,
            drbdRscDfnData,
            drbdSnapData.getNodeId(),
            drbdSnapData.getTcpPortList(),
            drbdSnapData.getPortCount(),
            drbdSnapData.getPeerSlots(),
            drbdSnapData.getAlStripes(),
            drbdSnapData.getAlStripeSize(),
            drbdSnapData.getFlags().getFlagsBits()
        );
    }

    @Override
    protected <RSC extends AbsResource<RSC>> DrbdVlmDfnData<Resource> restoreVlmDfnData(
        VolumeDefinition vlmDfnRef,
        VlmProviderObject<RSC> fromAbsRscVlmDataRef
    ) throws DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        String resourceNameSuffix = fromAbsRscVlmDataRef.getRscLayerObject().getResourceNameSuffix();
        return layerDataFactory.createDrbdVlmDfnData(
            vlmDfnRef,
            vlmDfnRef.getResourceDefinition().getName(),
            null,
            resourceNameSuffix,
            vlmDfnRef.getVolumeNumber(),
            null, // auto assign minor nr
            vlmDfnRef.getResourceDefinition().getLayerData(
                DeviceLayerKind.DRBD,
                resourceNameSuffix
            )
        );
    }

    @Override
    protected <RSC extends AbsResource<RSC>> DrbdVlmData<Resource> restoreVlmData(
        Volume vlmRef,
        DrbdRscData<Resource> rscDataRef,
        VlmProviderObject<RSC> vlmProviderObjectRef,
        Map<String, String> storpoolRenameMap,
        @Nullable ApiCallRc apiCallRc
    )
        throws DatabaseException, InvalidNameException
    {
        DrbdVlmData<RSC> drbdSnapVlmData = (DrbdVlmData<RSC>) vlmProviderObjectRef;
        return layerDataFactory.createDrbdVlmData(
            vlmRef,
            AbsLayerHelperUtils.getStorPool(
                vlmRef,
                rscDataRef,
                drbdSnapVlmData.getExternalMetaDataStorPool(),
                storpoolRenameMap,
                apiCallRc
            ),
            rscDataRef,
            vlmRef.getVolumeDefinition().getLayerData(
                DeviceLayerKind.DRBD,
                drbdSnapVlmData.getRscLayerObject().getResourceNameSuffix()
            )
        );
    }

    private NodeId getNodeId(@Nullable Integer nodeIdIntRef, DrbdRscDfnData<Resource> drbdRscDfnData)
        throws ExhaustedPoolException, ValueOutOfRangeException
    {
        NodeId nodeId;
        if (nodeIdIntRef == null)
        {
            int[] occupiedIds = new int[drbdRscDfnData.getDrbdRscDataList().size()];
            int idx = 0;
            for (DrbdRscData<Resource> drbdRscData : drbdRscDfnData.getDrbdRscDataList())
            {
                occupiedIds[idx] = drbdRscData.getNodeId().value;
                ++idx;
            }
            Arrays.sort(occupiedIds);
            nodeId = NodeIdAlloc.getFreeNodeId(occupiedIds);
        }
        else
        {
            nodeId = new NodeId(nodeIdIntRef);
        }
        return nodeId;
    }

    private short getPeerSlotsForNewResource(
        ResourceDefinition rscDfn,
        LayerPayload payload
    )
    {
        short peerSlots;

        try
        {
            Short payloadPeerSlots = payload.drbdRscDfn.peerSlotsNewResource;
            if (payloadPeerSlots == null)
            {
                String peerSlotsNewResourceProp = new PriorityProps(
                    rscDfn.getProps(),
                    rscDfn.getResourceGroup().getProps(),
                    ctrlPropsHelper.getStltPropsForView()
                ).getProp(ApiConsts.KEY_PEER_SLOTS_NEW_RESOURCE);
                peerSlots = peerSlotsNewResourceProp == null ?
                    InternalApiConsts.DEFAULT_PEER_SLOTS :
                    Short.valueOf(peerSlotsNewResourceProp);
            }
            else
            {
                peerSlots = payloadPeerSlots;
            }
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        return peerSlots;
    }

    /**
     * <p>
     * Checks if the given <code>peerSlots</code> is large enough for the given {@link ResourceDefinition} based on
     * {@link ResourceDefinition#getDiskfulCount()}. The <code>reservedPeerSlotCount</code> should be used
     * to indicate how much larger <code>peerSlots</code> should be than the count of already deployed diskful
     * resources. </p>
     * <p>Usual use-cases are
     * <ul>
     *  <li><code>0:</code> in case the given <code>rscDfn</code> should be cloned</li>
     *  <li><code>1: </code> if this method is called right before a new resource is created</li>
     * </ul></p>
     *
     * @param reservedPeerSlotCountRef if omitted {@value #DFLT_RESERVERD_PEER_SLOT_COUNT} is used.
     */
    private void checkPeerSlotCount(
        short peerSlots,
        @Nullable Short reservedPeerSlotCountRef,
        ResourceDefinition rscDfn
    )
    {
        short reservedPeerSlotCount = reservedPeerSlotCountRef == null ?
            DFLT_RESERVERD_PEER_SLOT_COUNT :
            reservedPeerSlotCountRef;
        if (peerSlots + reservedPeerSlotCount < rscDfn.getDiskfulCount() - 1)
        {
            StringBuilder details = new StringBuilder("Peerslot count '")
                .append(peerSlots)
                .append("' is too low since we already have '")
                .append(rscDfn.getDiskfulCount())
                .append("' diskful resources");
            if (reservedPeerSlotCount > 0)
            {
                details = details.append(" and we still need '")
                    .append(reservedPeerSlotCount)
                    .append("' for new resources");
            }
            throw new ApiRcException(
                ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_INSUFFICIENT_PEER_SLOTS,
                        "Insufficient peer slots to create resource"
                    )
                    .setDetails(details.toString())
                    .setCorrection("Configure a higher peer slot count on the resource definition or controller")
                    .build()
            );
        }
    }

    /**
     * Checks all existing DrbdRscData if their local peerSlot is at least 1 larger than the number of diskful
     * resources in order to add yet another diskful peer
     */
    private void checkIfPeersHaveEnoughPeerSlots(DrbdRscDfnData<Resource> drbdRscDfnDataRef)
    {
        TreeMap<Short, Set<NodeName>> peerSlots = new TreeMap<>();
        // we have not yet created the new DrbdRscData, so all currently existing diskful resources count
        // as peers. not -1 !
        short diskfulCount = 0;
        for (DrbdRscData<Resource> drbdRscData : drbdRscDfnDataRef.getDrbdRscDataList())
        {
            peerSlots.computeIfAbsent(drbdRscData.getPeerSlots(), ignore -> new TreeSet<>())
                .add(drbdRscData.getNodeName());
            if (drbdRscData.isDiskless())
            {
                diskfulCount++;
            }
        }

        // drop all entries with a key larger than diskfulCount
        peerSlots.tailMap(diskfulCount).clear();
        if (!peerSlots.isEmpty())
        {
            StringBuilder sb = new StringBuilder(
                "The resources on the following nodes have not enough peer slots:\n"
            );
            for (Entry<Short, Set<NodeName>> entry : peerSlots.entrySet())
            {
                short slots = entry.getKey();
                for (NodeName nodeName : entry.getValue())
                {
                    sb.append("   * ").append(nodeName.displayValue).append(" (").append(slots).append(")\n");
                }
            }
            sb.append(diskfulCount).append(" needed.");

            throw new ApiRcException(
                ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_INSUFFICIENT_PEER_SLOTS,
                        "Peers have insufficient peer slots to create new resource"
                    )
                    .setDetails(sb.toString())
                    .setCorrection("Recreate mentioned resources with higher peer slot")
                    .build()
            );
        }
    }
}
