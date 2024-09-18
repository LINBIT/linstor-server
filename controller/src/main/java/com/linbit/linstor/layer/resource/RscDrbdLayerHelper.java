package com.linbit.linstor.layer.resource;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.crypto.SecretGenerator;
import com.linbit.linstor.CtrlStorPoolResolveHelper;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.NodeIdAlloc;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.CoreModule.RemoteMap;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.controller.CtrlNodeApiCallHandler;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
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
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
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

import static com.linbit.linstor.core.apicallhandler.controller.CtrlVlmListApiCallHandler.getVlmDescriptionInline;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Singleton
public class RscDrbdLayerHelper extends
    AbsRscLayerHelper<
    DrbdRscData<Resource>, DrbdVlmData<Resource>,
    DrbdRscDfnData<Resource>, DrbdVlmDfnData<Resource>
>
{
    private final ReadOnlyProps stltConf;
    private final ResourceDefinitionRepository rscDfnMap;
    private final ModularCryptoProvider cryptoProvider;

    private final Provider<RscNvmeLayerHelper> nvmeHelperProvider;
    private final CtrlStorPoolResolveHelper storPoolResolveHelper;
    private final RemoteMap remoteMap;

    @Inject
    RscDrbdLayerHelper(
        ErrorReporter errorReporter,
        @ApiContext AccessContext apiCtx,
        ResourceDefinitionRepository rscDfnMapRef,
        LayerDataFactory layerDataFactory,
        @Named(LinStor.SATELLITE_PROPS) ReadOnlyProps stltConfRef,
        @Named(NumberPoolModule.LAYER_RSC_ID_POOL) DynamicNumberPool layerRscIdPool,
        Provider<CtrlRscLayerDataFactory> rscLayerDataFactory,
        Provider<RscNvmeLayerHelper> nvmeHelperProviderRef,
        CtrlStorPoolResolveHelper storPoolResolveHelperRef,
        ModularCryptoProvider cryptoProviderRef,
        RemoteMap remoteMapRef
    )
    {
        super(
            errorReporter,
            apiCtx,
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
        stltConf = stltConfRef;
        nvmeHelperProvider = nvmeHelperProviderRef;
        storPoolResolveHelper = storPoolResolveHelperRef;
        cryptoProvider = cryptoProviderRef;
        remoteMap = remoteMapRef;
    }

    @Override
    protected DrbdRscDfnData<Resource> createRscDfnData(
        ResourceDefinition rscDfn,
        String rscNameSuffix,
        LayerPayload payload
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
            ValueInUseException
    {
        DrbdRscDfnPayload drbdRscDfnPayload = payload.drbdRscDfn;

        TransportType transportType = drbdRscDfnPayload.transportType;
        String secret = drbdRscDfnPayload.sharedSecret;
        Short peerSlots = drbdRscDfnPayload.peerSlotsNewResource;
        Integer tcpPort = drbdRscDfnPayload.tcpPort;
        Integer alStripes = drbdRscDfnPayload.alStripes;
        Long alStripeSize = drbdRscDfnPayload.alStripeSize;
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
            peerSlots = getAndCheckPeerSlotsForNewResource(rscDfn, payload);
        }
        if (alStripes == null)
        {
            alStripes = InternalApiConsts.DEFAULT_AL_STRIPES;
        }
        if (alStripeSize == null)
        {
            alStripeSize = InternalApiConsts.DEFAULT_AL_SIZE;
        }

        checkPeerSlotCount(peerSlots, rscDfn);

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
        throws DatabaseException, ExhaustedPoolException, ValueOutOfRangeException, ValueInUseException,
        AccessDeniedException
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
            ResourceDefinition rscDfn = rscDfnMap.get(apiCtx, drbdRscDfnData.getResourceName());
            checkPeerSlotCount(drbdRscDfnPayload.peerSlotsNewResource, rscDfn);
            drbdRscDfnData.setPeerSlots(drbdRscDfnPayload.peerSlotsNewResource);
        }
    }

    @Override
    protected DrbdVlmDfnData<Resource> createVlmDfnData(
        VolumeDefinition vlmDfn,
        String rscNameSuffix,
        LayerPayload payload
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
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
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException, ImplementationError, InvalidNameException, LinStorException
    {
        ResourceDefinition rscDfn = rscRef.getResourceDefinition();
        DrbdRscDfnData<Resource> drbdRscDfnData = ensureResourceDefinitionExists(
            rscDfn,
            rscNameSuffixRef,
            payloadRef
        );

        NodeId nodeId = getNodeId(rscRef, payloadRef, rscNameSuffixRef, layerListRef, drbdRscDfnData, null);

        long initFlags = 0;
        // some flags that might already be set on resource level should be copied to DrbdRscData level
        if (isResourceDiskless(rscRef))
        {
            initFlags |= DrbdRscObject.DrbdRscFlags.DISKLESS.flagValue;
        }

        DrbdRscData<Resource> drbdRscData = layerDataFactory.createDrbdRscData(
            layerRscIdPool.autoAllocate(),
            rscRef,
            rscNameSuffixRef,
            parentObjectRef,
            drbdRscDfnData,
            nodeId,
            null,
            null,
            null,
            initFlags
        );
        drbdRscDfnData.getDrbdRscDataList().add(drbdRscData);
        return drbdRscData;
    }

    private boolean isResourceDiskless(Resource rscRef) throws AccessDeniedException
    {
        return rscRef.getStateFlags().isSomeSet(
            apiCtx,
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
        NodeId oldNodeIdRef
    )
        throws AccessDeniedException, InvalidNameException, DatabaseException, ImplementationError,
        ExhaustedPoolException, ValueOutOfRangeException
    {
        NodeId nodeId = oldNodeIdRef;

        boolean isNvmeBelow = layerListRef.contains(DeviceLayerKind.NVME);
        boolean isNvmeInitiator = rscRef.getStateFlags().isSet(apiCtx, Resource.Flags.NVME_INITIATOR);
        boolean isEbsInitiator = rscRef.getStateFlags().isSet(apiCtx, Resource.Flags.EBS_INITIATOR);

        Set<StorPool> allStorPools = layerDataHelperProvider.get()
            .getAllNeededStorPools(rscRef, payloadRef, layerListRef);
        Set<SharedStorPoolName> sharedStorPoolNames = allStorPools.stream()
            .map(StorPool::getSharedStorPoolName)
            .collect(Collectors.toSet());

        boolean isStoragePoolShared = areAllShared(allStorPools);

        if (isNvmeBelow && isNvmeInitiator || isEbsInitiator)
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
                    availabilityZones.add(RscStorageLayerHelper.getAvailabilityZone(apiCtx, remoteMap, sp));
                }
                if (availabilityZones.size() != 1)
                {
                    throw new ImplementationError(
                        "Unexpected count of availability zone. 1 entry expected. Found: " + availabilityZones +
                            " in storage pools: " + allStorPools
                    );
                }

                targetRsc = RscStorageLayerHelper.findTargetEbsResource(
                    apiCtx,
                    remoteMap,
                    rscRef.getResourceDefinition(),
                    availabilityZones.iterator().next(),
                    rscRef.getNode().getName().displayValue
                );
            }
            else
            {
                throw new ImplementationError("Unknown target type");
            }
            if (targetRsc != null)
            {
                AbsRscLayerObject<Resource> rootLayerData = targetRsc.getLayerData(apiCtx);
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
                Set<StorPool> peerStorPools = LayerVlmUtils.getStorPools(peerData.getAbsResource(), apiCtx);
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
        throws AccessDeniedException, DatabaseException, InvalidNameException, ImplementationError,
        ExhaustedPoolException, ValueOutOfRangeException
    {
        Resource rsc = drbdRscData.getAbsResource();

        if (isResourceDiskless(rsc))
        {
            drbdRscData.getFlags().enableFlags(apiCtx, DrbdRscFlags.DISKLESS);
        }
        else
        {
            drbdRscData.getFlags().disableFlags(apiCtx, DrbdRscFlags.DISKLESS);
        }

        NodeId oldNodeId = drbdRscData.getNodeId();
        NodeId newNodeId = getNodeId(
            rsc,
            payloadRef,
            drbdRscData.getResourceNameSuffix(),
            LayerRscUtils.getLayerStack(rsc, apiCtx),
            drbdRscData.getRscDfnLayerObject(),
            payloadRef.drbdRsc.needsNewNodeId ? null : oldNodeId
        );
        drbdRscData.setNodeId(newNodeId);
    }

    @Override
    protected boolean needsChildVlm(AbsRscLayerObject<Resource> childRscDataRef, Volume vlmRef)
        throws AccessDeniedException, InvalidKeyException
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

    private boolean isDrbdDiskless(AbsRscLayerObject<Resource> childRscDataRef) throws AccessDeniedException
    {
        StateFlags<Flags> rscFlags = childRscDataRef.getAbsResource().getStateFlags();
        return rscFlags.isSet(apiCtx, Resource.Flags.DRBD_DISKLESS) &&
            !rscFlags.isSomeSet(
                apiCtx,
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
        throws AccessDeniedException, InvalidNameException
    {
        Set<StorPool> storPools = new HashSet<>();
        Set<AbsRscLayerObject<Resource>> drbdRscDataSet = LayerRscUtils.getRscDataByLayer(
            rsc.getLayerData(apiCtx),
            DeviceLayerKind.DRBD
        );
        for (AbsRscLayerObject<Resource> drbdRscData : drbdRscDataSet)
        {
            if (needsMetaData((DrbdRscData<Resource>) drbdRscData, layerListRef))
            {
                StorPool metaStorPool = getMetaStorPool(rsc, vlmDfn, apiCtx);
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
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
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

        return drbdVlmData;
    }

    private StorPool getExternalMetaDiskStorPool(Volume vlm)
        throws InvalidKeyException, AccessDeniedException
    {
        String extMetaStorPoolNameStr = getExtMetaDataStorPoolName(vlm);
        StorPool extMetaStorPool = null;
        if (isExternalMetaDataPool(extMetaStorPoolNameStr))
        {
            try
            {
                extMetaStorPool = vlm.getAbsResource().getNode().getStorPool(
                    apiCtx,
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
        throws AccessDeniedException, InvalidKeyException
    {
        return isExternalMetaDataPool(getExtMetaDataStorPoolName(vlmRef));
    }

    private String getExtMetaDataStorPoolName(Volume vlmRef) throws InvalidKeyException, AccessDeniedException
    {
        VolumeDefinition vlmDfn = vlmRef.getVolumeDefinition();
        ResourceDefinition rscDfn = vlmRef.getResourceDefinition();
        ResourceGroup rscGrp = rscDfn.getResourceGroup();
        Resource rsc = vlmRef.getAbsResource();
        return new PriorityProps(
            vlmDfn.getProps(apiCtx),
            rscGrp.getVolumeGroupProps(apiCtx, vlmDfn.getVolumeNumber()),
            rsc.getProps(apiCtx),
            rscDfn.getProps(apiCtx),
            rscGrp.getProps(apiCtx),
            rsc.getNode().getProps(apiCtx)
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
        throws AccessDeniedException, InvalidKeyException
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
    ) throws AccessDeniedException
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

            Iterator<VolumeDefinition> iterateVolumeDfn = rscDfn.iterateVolumeDfn(apiCtx);
            ReadOnlyProps rscProps = rsc.getProps(apiCtx);
            ReadOnlyProps rscDfnProps = rscDfn.getProps(apiCtx);
            ReadOnlyProps rscGrpProps = rscDfn.getResourceGroup().getProps(apiCtx);
            ReadOnlyProps nodeProps = rsc.getNode().getProps(apiCtx);

            while (iterateVolumeDfn.hasNext())
            {
                String metaPool = new PriorityProps(
                    iterateVolumeDfn.next().getProps(apiCtx),
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
                apiCtx,
                Resource.Flags.DELETE,
                Resource.Flags.INACTIVATING,
                Resource.Flags.INACTIVE,
                Resource.Flags.INACTIVE_PERMANENTLY,
                Resource.Flags.DRBD_DISKLESS
            );
            boolean isNvmeBelow = layerListRef.contains(DeviceLayerKind.NVME);
            boolean isNvmeInitiator = rscFlags.isSet(apiCtx, Resource.Flags.NVME_INITIATOR);

            needsMetaDisk &= (!isNvmeBelow || isNvmeInitiator);
            ret = !allVlmsUseInternalMetaData && needsMetaDisk;
        }
        return ret;
    }

    @Override
    public StorPool getStorPool(Volume vlmRef, AbsRscLayerObject<Resource> childRef)
        throws AccessDeniedException, InvalidKeyException, InvalidNameException
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
        throws AccessDeniedException, DatabaseException, InvalidKeyException
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
        throws AccessDeniedException, DatabaseException
    {
        boolean changed = false;
        if (isDrbdDiskless(rscDataRef))
        {
            changed = addIgnoreReason(rscDataRef, LayerIgnoreReason.DRBD_DISKLESS, false, true, true);
        }
        if (rscDataRef.isSkipDiskEnabled(apiCtx, stltConf))
        {
            changed |= addIgnoreReason(rscDataRef, LayerIgnoreReason.DRBD_SKIP_DISK, false, true, true);
        }
        return changed;
    }

    public StorPool getMetaStorPool(Volume vlmRef, AccessContext accCtx)
        throws AccessDeniedException, InvalidNameException
    {
        return getMetaStorPool(vlmRef.getAbsResource(), vlmRef.getVolumeDefinition(), accCtx);
    }

    public StorPool getMetaStorPool(Resource rsc, VolumeDefinition vlmDfn, AccessContext accCtx)
        throws AccessDeniedException, InvalidNameException
    {
        return getMetaStorPool(rsc, vlmDfn, getPrioProps(rsc, vlmDfn, accCtx), accCtx);
    }

    private StorPool getMetaStorPool(
        Resource rsc,
        VolumeDefinition vlmDfn,
        PriorityProps prioProps,
        AccessContext accCtx
    )
        throws AccessDeniedException, InvalidNameException
    {
        StorPool metaStorPool = null;
        String metaStorPoolStr = prioProps.getProp(ApiConsts.KEY_STOR_POOL_DRBD_META_NAME);
        if (
            isExternalMetaDataPool(metaStorPoolStr) &&
                !rsc.getStateFlags().isSet(accCtx, Resource.Flags.DRBD_DISKLESS)
        )
        {
            metaStorPool = rsc.getNode().getStorPool(accCtx, new StorPoolName(metaStorPoolStr));
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

    private PriorityProps getPrioProps(Resource rsc, VolumeDefinition vlmDfn, AccessContext accCtx)
        throws AccessDeniedException
    {
        ResourceGroup rscGrp = vlmDfn.getResourceDefinition().getResourceGroup();
        Node node = rsc.getNode();
        return new PriorityProps(
            vlmDfn.getProps(accCtx),
            rscGrp.getVolumeGroupProps(accCtx, vlmDfn.getVolumeNumber()),
            rsc.getProps(accCtx),
            vlmDfn.getResourceDefinition().getProps(accCtx),
            rscGrp.getProps(accCtx),
            node.getProps(accCtx)
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
    ) throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        String resourceNameSuffix = fromSnapDataRef.getResourceNameSuffix();
        DrbdRscDfnData<RSC> dfnData;
        RSC absRsc = fromSnapDataRef.getAbsResource();
        if (absRsc instanceof Snapshot)
        {
            dfnData = ((Snapshot) absRsc).getSnapshotDefinition().getLayerData(
                apiCtx,
                DeviceLayerKind.DRBD,
                resourceNameSuffix
            );
        }
        else if (absRsc instanceof Resource)
        {
            dfnData = absRsc.getResourceDefinition().getLayerData(
                apiCtx,
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
    ) throws DatabaseException, AccessDeniedException, ExhaustedPoolException
    {
        DrbdRscData<Snapshot> drbdSnapData = (DrbdRscData<Snapshot>) fromAbsRscDataRef;
        String resourceNameSuffix = drbdSnapData.getResourceNameSuffix();
        DrbdRscDfnData<Resource> drbdRscDfnData = rscRef.getResourceDefinition().getLayerData(
            apiCtx,
            DeviceLayerKind.DRBD,
            resourceNameSuffix
        );

        DrbdRscData<Resource> drbdRscData = layerDataFactory.createDrbdRscData(
            layerRscIdPool.autoAllocate(),
            rscRef,
            resourceNameSuffix,
            rscParentRef,
            drbdRscDfnData,
            drbdSnapData.getNodeId(),
            drbdSnapData.getPeerSlots(),
            drbdSnapData.getAlStripes(),
            drbdSnapData.getAlStripeSize(),
            drbdSnapData.getFlags().getFlagsBits(apiCtx)
        );
        drbdRscDfnData.getDrbdRscDataList().add(drbdRscData);
        return drbdRscData;
    }

    @Override
    protected <RSC extends AbsResource<RSC>> DrbdVlmDfnData<Resource> restoreVlmDfnData(
        VolumeDefinition vlmDfnRef,
        VlmProviderObject<RSC> fromAbsRscVlmDataRef
    ) throws DatabaseException, AccessDeniedException, ValueOutOfRangeException, ExhaustedPoolException,
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
                apiCtx,
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
        throws DatabaseException, AccessDeniedException, InvalidNameException
    {
        DrbdVlmData<RSC> drbdSnapVlmData = (DrbdVlmData<RSC>) vlmProviderObjectRef;
        return layerDataFactory.createDrbdVlmData(
            vlmRef,
            AbsLayerHelperUtils.getStorPool(
                apiCtx,
                vlmRef,
                rscDataRef,
                drbdSnapVlmData.getExternalMetaDataStorPool(),
                storpoolRenameMap,
                apiCallRc
            ),
            rscDataRef,
            vlmRef.getVolumeDefinition().getLayerData(
                apiCtx,
                DeviceLayerKind.DRBD,
                drbdSnapVlmData.getRscLayerObject().getResourceNameSuffix()
            )
        );
    }

    private NodeId getNodeId(Integer nodeIdIntRef, DrbdRscDfnData<Resource> drbdRscDfnData)
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

    private short getAndCheckPeerSlotsForNewResource(
        ResourceDefinition rscDfn,
        LayerPayload payload
    )
        throws AccessDeniedException
    {
        short peerSlots;

        try
        {
            Short payloadPeerSlots = payload.drbdRscDfn.peerSlotsNewResource;
            if (payloadPeerSlots == null)
            {
                String peerSlotsNewResourceProp = new PriorityProps(
                    rscDfn.getProps(apiCtx),
                    rscDfn.getResourceGroup().getProps(apiCtx),
                    stltConf
                ).getProp(ApiConsts.KEY_PEER_SLOTS_NEW_RESOURCE);
                peerSlots = peerSlotsNewResourceProp == null ?
                    InternalApiConsts.DEFAULT_PEER_SLOTS :
                    Short.valueOf(peerSlotsNewResourceProp);
            }
            else
            {
                peerSlots = payloadPeerSlots;
            }
            checkPeerSlotCount(peerSlots, rscDfn);
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        return peerSlots;
    }

    private void checkPeerSlotCount(short peerSlots, ResourceDefinition rscDfn)
    {
        if (peerSlots < rscDfn.getResourceCount())
        {
            throw new ApiRcException(ApiCallRcImpl
                .entryBuilder(
                    ApiConsts.FAIL_INSUFFICIENT_PEER_SLOTS,
                    "Insufficient peer slots to create resource"
                )
                .setDetails("Peerslot count '" + peerSlots + "' is too low.")
                .setCorrection("Configure a higher peer slot count on the resource definition or controller")
                .build()
            );
        }
    }
}
