package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeId;
import com.linbit.linstor.NodeIdAlloc;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceData;
import com.linbit.linstor.ResourceDataFactory;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeData;
import com.linbit.linstor.VolumeDataFactory;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.core.CtrlObjectFactories;
import com.linbit.linstor.core.apicallhandler.AbsApiCallHandler;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSQLException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Provider;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.getRscDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlVlmApiCallHandler.getVlmDescriptionInline;

/**
 * Common API call handler base class for operations that create resources.
 */
abstract class CtrlRscCrtApiCallHandler extends AbsApiCallHandler
{
    private final Props stltConf;
    private final ResourceDataFactory resourceDataFactory;
    private final VolumeDataFactory volumeDataFactory;

    CtrlRscCrtApiCallHandler(
        ErrorReporter errorReporterRef,
        AccessContext apiCtxRef,
        CtrlStltSerializer interComSerializer,
        CtrlObjectFactories objectFactories,
        Provider<TransactionMgr> transMgrProviderRef,
        Provider<AccessContext> peerAccCtxRef,
        Provider<Peer> peerRef,
        WhitelistProps whitelistPropsRef,
        Props stltConfRef,
        ResourceDataFactory resourceDataFactoryRef,
        VolumeDataFactory volumeDataFactoryRef
    )
    {
        super(
            errorReporterRef,
            apiCtxRef,
            interComSerializer,
            objectFactories,
            transMgrProviderRef,
            peerAccCtxRef,
            peerRef,
            whitelistPropsRef
        );

        stltConf = stltConfRef;
        resourceDataFactory = resourceDataFactoryRef;
        volumeDataFactory = volumeDataFactoryRef;
    }

    protected NodeId getNextFreeNodeId(ResourceDefinitionData rscDfn)
    {
        NodeId freeNodeId;
        try
        {
            Iterator<Resource> rscIterator = rscDfn.iterateResource(peerAccCtx.get());
            int[] occupiedIds = new int[rscDfn.getResourceCount()];
            int idx = 0;
            while (rscIterator.hasNext())
            {
                occupiedIds[idx] = rscIterator.next().getNodeId().value;
                ++idx;
            }
            Arrays.sort(occupiedIds);

            freeNodeId = NodeIdAlloc.getFreeNodeId(occupiedIds);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "iterate the resources of resource definition '" + rscDfn.getName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        catch (ExhaustedPoolException exhaustedPoolExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_POOL_EXHAUSTED_NODE_ID,
                "An exception occured during generation of a node id."
            ), exhaustedPoolExc);
        }
        return freeNodeId;
    }

    protected ResourceData createResource(
        ResourceDefinitionData rscDfn,
        Node node,
        NodeId nodeId,
        List<String> flagList
    )
    {
        Resource.RscFlags[] flags = Resource.RscFlags.restoreFlags(
            FlagsHelper.fromStringList(
                Resource.RscFlags.class,
                flagList
            )
        );
        ResourceData rsc;
        try
        {
            checkPeerSlotsForNewPeer(rscDfn);
            short peerSlots = getAndCheckPeerSlotsForNewResource(rscDfn);

            rsc = resourceDataFactory.getInstance(
                peerAccCtx.get(),
                rscDfn,
                node,
                nodeId,
                flags,
                true, // persist this entry
                true // throw exception if the entry exists
            );

            rsc.getProps(peerAccCtx.get()).setProp(ApiConsts.KEY_PEER_SLOTS, Short.toString(peerSlots));
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "register the " + getRscDescriptionInline(node, rscDfn),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_EXISTS_RSC,
                "A " + getRscDescriptionInline(node, rscDfn) + " already exists."
            ), dataAlreadyExistsExc);
        }
        catch (InvalidValueException | InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        return rsc;
    }

    private void checkPeerSlotsForNewPeer(ResourceDefinitionData rscDfn)
        throws AccessDeniedException, InvalidKeyException
    {
        int resourceCount = rscDfn.getResourceCount();
        Iterator<Resource> rscIter = rscDfn.iterateResource(peerAccCtx.get());
        while (rscIter.hasNext())
        {
            Resource otherRsc = rscIter.next();

            String peerSlotsProp = otherRsc.getProps(peerAccCtx.get()).getProp(ApiConsts.KEY_PEER_SLOTS);
            short peerSlots = peerSlotsProp == null ?
                InternalApiConsts.DEFAULT_PEER_SLOTS :
                Short.valueOf(peerSlotsProp);

            if (peerSlots < resourceCount)
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INSUFFICIENT_PEER_SLOTS,
                    "Resource on node " + otherRsc.getAssignedNode().getName().displayValue +
                        " has insufficient peer slots to add another peer"
                ));
            }
        }
    }

    private short getAndCheckPeerSlotsForNewResource(ResourceDefinitionData rscDfn)
        throws InvalidKeyException, AccessDeniedException
    {
        int resourceCount = rscDfn.getResourceCount();

        String peerSlotsNewResourceProp = new PriorityProps(rscDfn.getProps(peerAccCtx.get()), stltConf)
            .getProp(ApiConsts.KEY_PEER_SLOTS_NEW_RESOURCE);
        short peerSlots = peerSlotsNewResourceProp == null ?
            InternalApiConsts.DEFAULT_PEER_SLOTS :
            Short.valueOf(peerSlotsNewResourceProp);

        if (peerSlots < resourceCount)
        {
            String detailsMsg = (peerSlotsNewResourceProp == null ? "Default" : "Configured") +
                " peer slot count " + peerSlots + " too low";
            throw new ApiRcException(ApiCallRcImpl
                .entryBuilder(ApiConsts.FAIL_INSUFFICIENT_PEER_SLOTS, "Insufficient peer slots to create resource")
                .setDetails(detailsMsg)
                .setCorrection("Configure a higher peer slot count on the resource definition or controller")
                .build()
            );
        }
        return peerSlots;
    }

    protected VolumeData createVolume(
        Resource rsc,
        VolumeDefinition vlmDfn,
        StorPool storPool,
        Volume.VlmApi vlmApi
    )
    {
        VolumeData vlm;
        try
        {
            String blockDevice = vlmApi == null ? null : vlmApi.getBlockDevice();
            String metaDisk = vlmApi == null ? null : vlmApi.getMetaDisk();

            vlm = volumeDataFactory.getInstance(
                peerAccCtx.get(),
                rsc,
                vlmDfn,
                storPool,
                blockDevice,
                metaDisk,
                null, // flags
                true, // persist this entry
                true // throw exception if the entry exists
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "register " + getVlmDescriptionInline(rsc, vlmDfn),
                ApiConsts.FAIL_ACC_DENIED_VLM
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_EXISTS_VLM,
                "The " + getVlmDescriptionInline(rsc, vlmDfn) + " already exists"
            ), dataAlreadyExistsExc);
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
        return vlm;
    }

    protected Iterator<VolumeDefinition> getVlmDfnIterator(ResourceDefinitionData rscDfn)
    {
        Iterator<VolumeDefinition> iterator;
        try
        {
            iterator = rscDfn.iterateVolumeDfn(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return iterator;
    }
}
