package com.linbit.linstor.layer;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeId;
import com.linbit.linstor.NodeIdAlloc;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.Resource;
import com.linbit.linstor.Resource.RscFlags;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.SecretGenerator;
import com.linbit.linstor.core.apicallhandler.controller.CtrlNodeApiCallHandler;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.layer.LayerPayload.DrbdRscDfnPayload;
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
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscObject.DrbdRscFlags;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerDataFactory;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlVlmListApiCallHandler.getVlmDescriptionInline;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

class DrbdLayerHelper extends AbsLayerHelper<DrbdRscData, DrbdVlmData, DrbdRscDfnData, DrbdVlmDfnData>
{
    private final Props stltConf;

    @Inject
    DrbdLayerHelper(
        ErrorReporter errorReporter,
        @ApiContext AccessContext apiCtx,
        LayerDataFactory layerDataFactory,
        @Named(LinStor.SATELLITE_PROPS)Props stltConfRef,
        @Named(NumberPoolModule.LAYER_RSC_ID_POOL) DynamicNumberPool layerRscIdPool,
        Provider<CtrlLayerDataHelper> layerHelperProviderRef
    )
    {
        super(
            errorReporter,
            apiCtx,
            layerDataFactory,
            layerRscIdPool,
            DrbdRscData.class,
            DeviceLayerKind.DRBD,
            layerHelperProviderRef
        );
        stltConf = stltConfRef;
    }

    @Override
    protected DrbdRscDfnData createRscDfnData(
        ResourceDefinition rscDfn,
        String rscNameSuffix,
        LayerPayload payload
    )
        throws AccessDeniedException, SQLException, ValueOutOfRangeException, ExhaustedPoolException,
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
            rscDfn,
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
    protected void mergeRscDfnData(DrbdRscDfnData drbdRscDfnData, LayerPayload payload)
        throws SQLException, ExhaustedPoolException, ValueOutOfRangeException, ValueInUseException
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
            checkPeerSlotCount(drbdRscDfnPayload.peerSlotsNewResource, drbdRscDfnData.getResourceDefinition());
            drbdRscDfnData.setPeerSlots(drbdRscDfnPayload.peerSlotsNewResource);
        }
    }

    @Override
    protected DrbdVlmDfnData createVlmDfnData(
        VolumeDefinition vlmDfn,
        String rscNameSuffix,
        LayerPayload payload
    )
        throws AccessDeniedException, SQLException, ValueOutOfRangeException, ExhaustedPoolException,
            ValueInUseException
    {
        DrbdRscDfnData drbdRscDfnData = ensureResourceDefinitionExists(
            vlmDfn.getResourceDefinition(),
            rscNameSuffix,
            payload
        );
        return layerDataFactory.createDrbdVlmDfnData(
            vlmDfn,
            rscNameSuffix,
            payload.drbdVlmDfn.minorNr,
            drbdRscDfnData
        );
    }

    @Override
    protected void mergeVlmDfnData(DrbdVlmDfnData vlmDfnDataRef, LayerPayload payloadRef)
    {
        // minor number cannot be changed

        // no-op
    }

    @Override
    protected DrbdRscData createRscData(
        Resource rscRef,
        LayerPayload payloadRef,
        String rscNameSuffixRef,
        RscLayerObject parentObjectRef
    )
        throws AccessDeniedException, SQLException, ValueOutOfRangeException, ExhaustedPoolException,
            ValueInUseException
    {
        ResourceDefinition rscDfn = rscRef.getDefinition();
        DrbdRscDfnData drbdRscDfnData = ensureResourceDefinitionExists(
            rscDfn,
            rscNameSuffixRef,
            payloadRef
        );

        NodeId nodeId = getNodeId(payloadRef.drbdRsc.nodeId, drbdRscDfnData);

        DrbdRscData drbdRscData = layerDataFactory.createDrbdRscData(
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
    protected void mergeRscData(DrbdRscData drbdRscData, LayerPayload payloadRef)
        throws AccessDeniedException, SQLException
    {
        drbdRscData.getFlags().resetFlagsTo(
            apiCtx,
            DrbdRscFlags.restoreFlags(
                drbdRscData.getResource().getStateFlags().getFlagsBits(apiCtx)
            )
        );
    }

    @Override
    protected boolean needsChildVlm(RscLayerObject childRscDataRef, Volume vlmRef)
        throws AccessDeniedException, InvalidKeyException
    {
        boolean needsChild;
        RscLayerObject drbdRscData = childRscDataRef.getParent();
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
    protected DrbdVlmData createVlmLayerData(
        DrbdRscData drbdRscData,
        Volume vlm,
        LayerPayload payload
    )
        throws AccessDeniedException, SQLException, ValueOutOfRangeException, ExhaustedPoolException,
            ValueInUseException, LinStorException, InvalidKeyException
    {
        DrbdVlmDfnData drbdVlmDfnData = ensureVolumeDefinitionExists(
            vlm.getVolumeDefinition(),
            drbdRscData.getResourceNameSuffix(),
            payload
        );
        String extMetaStorPoolNameStr = getExtMetaDataStorPoolName(vlm);
        StorPool extMetaStorPool = null;
        if (isExternalMetaDataPool(extMetaStorPoolNameStr))
        {
            try
            {
                extMetaStorPool = vlm.getResource().getAssignedNode().getStorPool(
                    apiCtx,
                    new StorPoolName(extMetaStorPoolNameStr)
                );
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
        DrbdVlmData drbdVlmData = layerDataFactory.createDrbdVlmData(
            vlm,
            extMetaStorPool,
            drbdRscData,
            drbdVlmDfnData
        );

        return drbdVlmData;
    }

    @Override
    protected void mergeVlmData(DrbdVlmData drbdVlmData, Volume vlmRef, LayerPayload payloadRef)
        throws AccessDeniedException, InvalidKeyException
    {
        // nothing to do
    }

    private boolean isUsingExternalMetaData(Volume vlmRef)
        throws AccessDeniedException, InvalidKeyException
    {
        return isExternalMetaDataPool(getExtMetaDataStorPoolName(vlmRef));
    }

    private String getExtMetaDataStorPoolName(Volume vlmRef) throws InvalidKeyException, AccessDeniedException
    {
        return new PriorityProps(
            vlmRef.getVolumeDefinition().getProps(apiCtx),
            vlmRef.getResource().getProps(apiCtx),
            vlmRef.getResourceDefinition().getProps(apiCtx),
            vlmRef.getResource().getAssignedNode().getProps(apiCtx)
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
    protected List<String> getRscNameSuffixes(DrbdRscData rscDataRef)
        throws AccessDeniedException, InvalidKeyException
    {
        boolean allVlmsUseInternalMetaData = true;
        Resource rsc = rscDataRef.getResource();
        ResourceDefinition rscDfn = rsc.getDefinition();

        Iterator<VolumeDefinition> iterateVolumeDfn = rscDfn.iterateVolumeDfn(apiCtx);
        Props rscProps = rsc.getProps(apiCtx);
        Props rscDfnProps = rscDfn.getProps(apiCtx);
        Props nodeProps = rsc.getAssignedNode().getProps(apiCtx);

        while (iterateVolumeDfn.hasNext())
        {
            String metaPool = new PriorityProps(
                iterateVolumeDfn.next().getProps(apiCtx),
                rscProps,
                rscDfnProps,
                nodeProps
            ).getProp(ApiConsts.KEY_STOR_POOL_DRBD_META_NAME);
            if (isExternalMetaDataPool(metaPool))
            {
                allVlmsUseInternalMetaData = false;
                break;
            }
        }

        List<String> ret;
        if (allVlmsUseInternalMetaData)
        {
            ret = Arrays.asList("");
        }
        else
        {
            ret = Arrays.asList("", DrbdRscData.SUFFIX_META);
        }

        return ret;
    }

    @Override
    protected StorPool getStorPool(Volume vlmRef, RscLayerObject childRef)
        throws AccessDeniedException, InvalidKeyException, InvalidNameException
    {
        StorPool metaStorPool = null;
        if (childRef.getSuffixedResourceName().contains(DrbdRscData.SUFFIX_META))
        {
            VolumeDefinition vlmDfn = vlmRef.getVolumeDefinition();
            Resource rsc = vlmRef.getResource();
            Node node = rsc.getAssignedNode();
            PriorityProps prioProps = new PriorityProps(
                vlmDfn.getProps(apiCtx),
                rsc.getProps(apiCtx),
                vlmDfn.getResourceDefinition().getProps(apiCtx),
                node.getProps(apiCtx)
            );

            String metaStorPoolStr = prioProps.getProp(ApiConsts.KEY_STOR_POOL_DRBD_META_NAME);
            if (
                isExternalMetaDataPool(metaStorPoolStr) &&
                rsc.getStateFlags().isUnset(apiCtx, RscFlags.DISKLESS)
            )
            {
                metaStorPool = node.getStorPool(apiCtx, new StorPoolName(metaStorPoolStr));
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
        return metaStorPool;
    }

    private NodeId getNodeId(Integer nodeIdIntRef, DrbdRscDfnData drbdRscDfnData)
        throws ExhaustedPoolException, ValueOutOfRangeException
    {
        NodeId nodeId;
        if (nodeIdIntRef == null)
        {
            int[] occupiedIds = new int[drbdRscDfnData.getDrbdRscDataList().size()];
            int idx = 0;
            for (DrbdRscData drbdRscData : drbdRscDfnData.getDrbdRscDataList())
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
