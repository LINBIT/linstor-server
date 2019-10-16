package com.linbit.linstor.layer.resource;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.NodeIdAlloc;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.SecretGenerator;
import com.linbit.linstor.core.apicallhandler.controller.CtrlNodeApiCallHandler;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.core.types.NodeId;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.layer.LayerPayload.DrbdRscDfnPayload;
import com.linbit.linstor.layer.resource.CtrlRscLayerDataFactory.ChildResourceData;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmDfnData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject.TransportType;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscObject.DrbdRscFlags;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerDataFactory;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlVlmListApiCallHandler.getVlmDescriptionInline;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

@Singleton
public class RscDrbdLayerHelper extends
    AbsRscLayerHelper<
    DrbdRscData<Resource>, DrbdVlmData<Resource>,
    DrbdRscDfnData<Resource>, DrbdVlmDfnData<Resource>
>
{
    private final Props stltConf;
    private final ResourceDefinitionRepository rscDfnMap;

    @Inject
    RscDrbdLayerHelper(
        ErrorReporter errorReporter,
        @ApiContext AccessContext apiCtx,
        ResourceDefinitionRepository rscDfnMapRef,
        LayerDataFactory layerDataFactory,
        @Named(LinStor.SATELLITE_PROPS)Props stltConfRef,
        @Named(NumberPoolModule.LAYER_RSC_ID_POOL) DynamicNumberPool layerRscIdPool,
        Provider<CtrlRscLayerDataFactory> rscLayerDataFactory
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
        if (secret == null)
        {
            secret = SecretGenerator.generateSharedSecret();
        }
        if (transportType == null)
        {
            transportType = TransportType.IP;
        }
        if (peerSlots == null)
        {
            peerSlots = getAndCheckPeerSlotsForNewResource(rscDfn, payload);
        }

        checkPeerSlotCount(peerSlots, rscDfn);

        return layerDataFactory.createDrbdRscDfnData(
            rscDfn.getName(),
            null,
            rscNameSuffix,
            peerSlots,
            InternalApiConsts.DEFAULT_AL_STRIPES,
            InternalApiConsts.DEFAULT_AL_SIZE,
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
            ValueInUseException
    {
        DrbdRscDfnData<Resource> drbdRscDfnData = ensureResourceDefinitionExists(
            vlmDfn.getResourceDefinition(),
            rscNameSuffix,
            payload
        );
        return layerDataFactory.createDrbdVlmDfnData(
            vlmDfn,
            null,
            rscNameSuffix,
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
            ValueInUseException
    {
        ResourceDefinition rscDfn = rscRef.getDefinition();
        DrbdRscDfnData<Resource> drbdRscDfnData = ensureResourceDefinitionExists(
            rscDfn,
            rscNameSuffixRef,
            payloadRef
        );

        NodeId nodeId = getNodeId(payloadRef.drbdRsc.nodeId, drbdRscDfnData);

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
            rscRef.getStateFlags().getFlagsBits(apiCtx)
        );
        drbdRscDfnData.getDrbdRscDataList().add(drbdRscData);
        return drbdRscData;
    }

    @Override
    protected void mergeRscData(DrbdRscData<Resource> drbdRscData, LayerPayload payloadRef)
        throws AccessDeniedException, DatabaseException
    {
        drbdRscData.getFlags().resetFlagsTo(
            apiCtx,
            DrbdRscFlags.restoreFlags(
                drbdRscData.getAbsResource().getStateFlags().getFlagsBits(apiCtx)
            )
        );
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
        StorPool extMetaStorPool = getExternalMetaDiskStorPool(vlm);
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
        boolean allVlmsUseInternalMetaData = true;
        Resource rsc = rscDataRef.getAbsResource();
        ResourceDefinition rscDfn = rsc.getDefinition();

        Iterator<VolumeDefinition> iterateVolumeDfn = rscDfn.iterateVolumeDfn(apiCtx);
        Props rscProps = rsc.getProps(apiCtx);
        Props rscDfnProps = rscDfn.getProps(apiCtx);
        Props rscGrpProps = rscDfn.getResourceGroup().getProps(apiCtx);
        Props nodeProps = rsc.getNode().getProps(apiCtx);

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

        List<ChildResourceData> ret = new ArrayList<>();
        ret.add(new ChildResourceData("")); // always have data

        boolean isNvmeBelow = layerListRef.contains(DeviceLayerKind.NVME);
        boolean isNvmeInitiator = rscDataRef.getAbsResource().getStateFlags()
            .isSet(apiCtx, Resource.Flags.NVME_INITIATOR);
        boolean isDrbdDiskless = rsc.getStateFlags().isSet(apiCtx, Resource.Flags.DRBD_DISKLESS);

        if (!allVlmsUseInternalMetaData && !isDrbdDiskless && (!isNvmeBelow || isNvmeInitiator))
        {
            ret.add(new ChildResourceData(DrbdRscData.SUFFIX_META, DeviceLayerKind.STORAGE));
        }

        return ret;
    }

    @Override
    public StorPool getStorPool(Volume vlmRef, AbsRscLayerObject<Resource> childRef)
        throws AccessDeniedException, InvalidKeyException, InvalidNameException
    {
        StorPool metaStorPool = null;
        if (childRef.getSuffixedResourceName().contains(DrbdRscData.SUFFIX_META))
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

    public StorPool getMetaStorPool(Volume vlmRef, AccessContext accCtx)
        throws AccessDeniedException, InvalidNameException
    {
        StorPool metaStorPool = null;
        try
        {
            VolumeDefinition vlmDfn = vlmRef.getVolumeDefinition();
            Resource rsc = vlmRef.getAbsResource();
            Node node = rsc.getNode();
            ResourceGroup rscGrp = vlmDfn.getResourceDefinition().getResourceGroup();
            PriorityProps prioProps = new PriorityProps(
                vlmDfn.getProps(accCtx),
                rscGrp.getVolumeGroupProps(accCtx, vlmDfn.getVolumeNumber()),
                rsc.getProps(accCtx),
                vlmDfn.getResourceDefinition().getProps(accCtx),
                rscGrp.getProps(accCtx),
                node.getProps(accCtx)
            );

            String metaStorPoolStr = prioProps.getProp(ApiConsts.KEY_STOR_POOL_DRBD_META_NAME);
            if (
                isExternalMetaDataPool(metaStorPoolStr) &&
                !rsc.getStateFlags().isSet(accCtx, Resource.Flags.DRBD_DISKLESS)
            )
            {
                metaStorPool = node.getStorPool(accCtx, new StorPoolName(metaStorPoolStr));
                if (metaStorPool == null)
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_NOT_FOUND_STOR_POOL,
                            "Configured (meta) storage pool '" + metaStorPoolStr + "' not found on " +
                            CtrlNodeApiCallHandler.getNodeDescriptionInline(node))
                    );
                }
            }
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError("Invalid hardcoded property key");
        }
        return metaStorPool;
    }

    @Override
    protected boolean isExpectedToProvideDevice(DrbdRscData<Resource> drbdRscData)
    {
        return true;
    }

    @Override
    protected DrbdRscDfnData<Resource> restoreRscDfnData(
        ResourceDefinition rscDfnRef,
        AbsRscLayerObject<Snapshot> fromSnapDataRef
    ) throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        String resourceNameSuffix = fromSnapDataRef.getResourceNameSuffix();
        DrbdRscDfnData<Snapshot> snapDfnData = fromSnapDataRef.getAbsResource().getSnapshotDefinition().getLayerData(
            apiCtx,
            DeviceLayerKind.DRBD,
            resourceNameSuffix
        );

        return layerDataFactory.createDrbdRscDfnData(
            rscDfnRef.getName(),
            null,
            resourceNameSuffix,
            snapDfnData.getPeerSlots(),
            snapDfnData.getAlStripes(),
            snapDfnData.getAlStripeSize(),
            null,
            snapDfnData.getTransportType(),
            null
        );
    }

    @Override
    protected DrbdRscData<Resource> restoreRscData(
        Resource rscRef,
        AbsRscLayerObject<Snapshot> fromSnapDataRef,
        AbsRscLayerObject<Resource> rscParentRef
    ) throws DatabaseException, AccessDeniedException, ExhaustedPoolException
    {
        DrbdRscData<Snapshot> drbdSnapData = (DrbdRscData<Snapshot>) fromSnapDataRef;
        String resourceNameSuffix = drbdSnapData.getResourceNameSuffix();
        DrbdRscDfnData<Resource> drbdRscDfnData = rscRef.getDefinition().getLayerData(
            apiCtx,
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
            drbdSnapData.getPeerSlots(),
            drbdSnapData.getAlStripes(),
            drbdSnapData.getAlStripeSize(),
            drbdSnapData.getFlags().getFlagsBits(apiCtx)
        );
    }

    @Override
    protected DrbdVlmDfnData<Resource> restoreVlmDfnData(
        VolumeDefinition vlmDfnRef,
        VlmProviderObject<Snapshot> fromSnapVlmDataRef
    ) throws DatabaseException, AccessDeniedException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        String resourceNameSuffix = fromSnapVlmDataRef.getRscLayerObject().getResourceNameSuffix();
        return layerDataFactory.createDrbdVlmDfnData(
            vlmDfnRef,
            fromSnapVlmDataRef.getRscLayerObject().getAbsResource().getSnapshotName(),
            resourceNameSuffix,
            null,
            vlmDfnRef.getResourceDefinition().getLayerData(
                apiCtx,
                DeviceLayerKind.DRBD,
                resourceNameSuffix
            )
        );
    }

    @Override
    protected DrbdVlmData<Resource> restoreVlmData(
        Volume vlmRef,
        DrbdRscData<Resource> rscDataRef,
        VlmProviderObject<Snapshot> vlmProviderObjectRef
    )
        throws DatabaseException, AccessDeniedException
    {
        DrbdVlmData<Snapshot> drbdSnapVlmData = (DrbdVlmData<Snapshot>) vlmProviderObjectRef;
        return layerDataFactory.createDrbdVlmData(
            vlmRef,
            drbdSnapVlmData.getExternalMetaDataStorPool(),
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
