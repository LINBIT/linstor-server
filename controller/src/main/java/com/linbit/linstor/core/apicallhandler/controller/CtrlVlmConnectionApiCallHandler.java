package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdater;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeConnection;
import com.linbit.linstor.core.objects.VolumeConnectionFactory;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

@Singleton
class CtrlVlmConnectionApiCallHandler
{
    private final AccessContext apiCtx;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final VolumeConnectionFactory volumeConnectionFactory;
    private final CtrlSatelliteUpdater ctrlSatelliteUpdater;
    private final ResponseConverter responseConverter;
    private final Provider<Peer> peer;
    private final Provider<AccessContext> peerAccCtx;

    @Inject
    CtrlVlmConnectionApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        VolumeConnectionFactory volumeConnectionFactoryRef,
        CtrlSatelliteUpdater ctrlSatelliteUpdaterRef,
        ResponseConverter responseConverterRef,
        Provider<Peer> peerRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef
    )
    {
        apiCtx = apiCtxRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        volumeConnectionFactory = volumeConnectionFactoryRef;
        ctrlSatelliteUpdater = ctrlSatelliteUpdaterRef;
        responseConverter = responseConverterRef;
        peer = peerRef;
        peerAccCtx = peerAccCtxRef;
    }

    public ApiCallRc createVolumeConnection(
        String nodeName1Str,
        String nodeName2Str,
        String rscNameStr,
        int vlmNrInt,
        Map<String, String> vlmConnPropsMap
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeVlmConnectionContext(
            ApiOperation.makeCreateOperation(),
            nodeName1Str,
            nodeName2Str,
            rscNameStr,
            vlmNrInt
        );

        try
        {
            VolumeConnection vlmConn = createVlmConn(nodeName1Str, nodeName2Str, rscNameStr, vlmNrInt);

            ctrlPropsHelper.fillProperties(
                responses,
                LinStorObject.VLM_CONN,
                vlmConnPropsMap,
                getProps(vlmConn),
                ApiConsts.FAIL_ACC_DENIED_VLM_CONN);

            ctrlTransactionHelper.commit();

            responseConverter.addWithOp(
                responses, context,
                ApiSuccessUtils.defaultCreatedEntry(
                    vlmConn.getUuid(), getVlmConnectionDescriptionInline(apiCtx, vlmConn)
                )
            );
            responseConverter.addWithDetail(responses, context, updateSatellites(vlmConn));
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    public ApiCallRc modifyVolumeConnection(
        UUID rscConnUuid,
        String nodeName1Str,
        String nodeName2Str,
        String rscNameStr,
        int vlmNrInt,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        Set<String> deletePropNamespaces
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        boolean notifyStlts;
        ResponseContext context = makeVlmConnectionContext(
            ApiOperation.makeModifyOperation(),
            nodeName1Str,
            nodeName2Str,
            rscNameStr,
            vlmNrInt
        );

        try
        {
            VolumeConnection vlmConn = loadVlmConn(nodeName1Str, nodeName2Str, rscNameStr, vlmNrInt);

            if (rscConnUuid != null && !rscConnUuid.equals(vlmConn.getUuid()))
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_UUID_VLM_CONN,
                        "UUID-check failed"
                    )
                );
            }

            Props props = getProps(vlmConn);

            notifyStlts = ctrlPropsHelper.fillProperties(
                responses,
                LinStorObject.VLM_CONN,
                overrideProps,
                getProps(vlmConn),
                ApiConsts.FAIL_ACC_DENIED_VLM_CONN);
            notifyStlts = ctrlPropsHelper.remove(
                responses, LinStorObject.VLM_CONN, props, deletePropKeys, deletePropNamespaces) || notifyStlts;

            ctrlTransactionHelper.commit();

            responseConverter.addWithOp(
                responses, context,
                ApiSuccessUtils.defaultModifiedEntry(
                    vlmConn.getUuid(), getVlmConnectionDescriptionInline(apiCtx, vlmConn)
                )
            );
            if (notifyStlts)
            {
                responseConverter.addWithDetail(responses, context, updateSatellites(vlmConn));
            }
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    public ApiCallRc deleteVolumeConnection(
        String nodeName1Str,
        String nodeName2Str,
        String rscNameStr,
        int vlmNrInt
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeVlmConnectionContext(
            ApiOperation.makeDeleteOperation(),
            nodeName1Str,
            nodeName2Str,
            rscNameStr,
            vlmNrInt
        );

        try
        {
            VolumeConnection vlmConn = loadVlmConn(nodeName1Str, nodeName2Str, rscNameStr, vlmNrInt);
            if (vlmConn == null)
            {
                responseConverter.addWithDetail(
                    responses, context,
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.WARN_NOT_FOUND,
                        "Could not delete " +
                            getVlmConnectionDescriptionInline(nodeName1Str, nodeName2Str, rscNameStr, vlmNrInt) +
                            " as it does not exist."
                    )
                );
            }
            else
            {
                UUID vlmConnUuid = vlmConn.getUuid();
                delete(vlmConn);
                ctrlTransactionHelper.commit();

                responseConverter.addWithOp(
                    responses, context,
                    ApiSuccessUtils.defaultDeletedEntry(
                        vlmConnUuid, getVlmConnectionDescription(nodeName1Str, nodeName2Str, rscNameStr, vlmNrInt)
                    )
                );
            }
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    private VolumeConnection createVlmConn(
        String nodeName1Str,
        String nodeName2Str,
        String rscNameStr,
        int vlmNrInt
    )
    {
        Node node1 = ctrlApiDataLoader.loadNode(nodeName1Str, true);
        Node node2 = ctrlApiDataLoader.loadNode(nodeName2Str, true);

        Resource rsc1 = getRsc(node1, rscNameStr);
        Resource rsc2 = getRsc(node2, rscNameStr);

        Volume sourceVolume = getVlm(rsc1, vlmNrInt);
        Volume targetVolume = getVlm(rsc2, vlmNrInt);

        VolumeConnection vlmConn;
        try
        {
            vlmConn = volumeConnectionFactory.create(
                peerAccCtx.get(),
                sourceVolume,
                targetVolume
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "create " + getVlmConnectionDescriptionInline(nodeName1Str, nodeName2Str, rscNameStr, vlmNrInt),
                ApiConsts.FAIL_ACC_DENIED_VLM_CONN
            );
        }
        catch (LinStorDataAlreadyExistsException alreadyExistsExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_EXISTS_VLM_CONN,
                getVlmConnectionDescription(nodeName1Str, nodeName2Str, rscNameStr, vlmNrInt) + " already exists",
                true
            ), alreadyExistsExc);
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
        return vlmConn;
    }

    private @Nullable VolumeConnection loadVlmConn(
        String nodeName1,
        String nodeName2,
        String rscNameStr,
        int vlmNr
    )
    {
        Node node1 = ctrlApiDataLoader.loadNode(nodeName1, true);
        Node node2 = ctrlApiDataLoader.loadNode(nodeName2, true);

        Resource rsc1 = getRsc(node1, rscNameStr);
        Resource rsc2 = getRsc(node2, rscNameStr);

        Volume vlm1 = getVlm(rsc1, vlmNr);
        Volume vlm2 = getVlm(rsc2, vlmNr);

        VolumeConnection vlmConn;
        try
        {
            vlmConn = VolumeConnection.get(
                peerAccCtx.get(),
                vlm1,
                vlm2
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access " + getVlmConnectionDescriptionInline(nodeName1, nodeName2, rscNameStr, vlmNr),
                ApiConsts.FAIL_ACC_DENIED_VLM_CONN
            );
        }
        return vlmConn;
    }

    private Resource getRsc(Node node, String rscNameStr)
    {
        Resource rsc;
        try
        {
            rsc = node.getResource(peerAccCtx.get(), LinstorParsingUtils.asRscName(rscNameStr));
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access resource '" + rscNameStr + "' from node '" + node.getName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        return rsc;
    }

    private Volume getVlm(Resource rsc, int vlmNr)
    {
        return rsc.getVolume(LinstorParsingUtils.asVlmNr(vlmNr));
    }

    private Props getProps(VolumeConnection vlmConn)
    {
        Props props;
        try
        {
            props = vlmConn.getProps(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access properties of " + getVlmConnectionDescriptionInline(apiCtx, vlmConn),
                ApiConsts.FAIL_ACC_DENIED_VLM_CONN
            );
        }
        return props;
    }

    private void delete(VolumeConnection vlmConn)
    {
        try
        {
            vlmConn.delete(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "delete " + getVlmConnectionDescriptionInline(apiCtx, vlmConn),
                ApiConsts.FAIL_ACC_DENIED_VLM_CONN
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private ApiCallRcImpl updateSatellites(VolumeConnection vlmConn)
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        try
        {
            responses.addEntries(
                ctrlSatelliteUpdater.updateSatellites(vlmConn.getSourceVolume(apiCtx).getResourceDefinition()));
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }

        return responses;
    }

    private static String getVlmConnectionDescription(
        String nodeName1Str,
        String nodeName2Str,
        String rscNameStr,
        int vlmNr
    )
    {
        return "Volume connection between nodes " + nodeName1Str + " and " +
            nodeName2Str + " on resource " + rscNameStr + " on volume number " +
            vlmNr;
    }

    private static String getVlmConnectionDescriptionInline(AccessContext accCtx, VolumeConnection vlmConn)
    {
        String descriptionInline;
        try
        {
            descriptionInline = getVlmConnectionDescriptionInline(
                vlmConn.getSourceVolume(accCtx).getAbsResource().getNode().getName().displayValue,
                vlmConn.getTargetVolume(accCtx).getAbsResource().getNode().getName().displayValue,
                vlmConn.getSourceVolume(accCtx).getResourceDefinition().getName().displayValue,
                vlmConn.getSourceVolume(accCtx).getVolumeDefinition().getVolumeNumber().value
            );
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return descriptionInline;
    }

    private static String getVlmConnectionDescriptionInline(
        String nodeName1Str,
        String nodeName2Str,
        String rscNameStr,
        int vlmNr
    )
    {
        return "volume connection between nodes '" + nodeName1Str + "' and '" +
            nodeName2Str + "' on resource '" + rscNameStr + "' on volume number '" +
            vlmNr + "'";
    }

    private static ResponseContext makeVlmConnectionContext(
        ApiOperation operation,
        String nodeName1Str,
        String nodeName2Str,
        String rscNameStr,
        int vlmNrInt
    )
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_1ST_NODE, nodeName1Str);
        objRefs.put(ApiConsts.KEY_2ND_NODE, nodeName2Str);
        objRefs.put(ApiConsts.KEY_RSC_DFN, rscNameStr);
        objRefs.put(ApiConsts.KEY_VLM_NR, Integer.toString(vlmNrInt));

        return new ResponseContext(
            operation,
            getVlmConnectionDescription(nodeName1Str, nodeName2Str, rscNameStr, vlmNrInt),
            getVlmConnectionDescriptionInline(nodeName1Str, nodeName2Str, rscNameStr, vlmNrInt),
            ApiConsts.MASK_VLM_CONN,
            objRefs
        );
    }
}
