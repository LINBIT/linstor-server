package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceConnection;
import com.linbit.linstor.ResourceConnectionData;
import com.linbit.linstor.ResourceConnectionDataControllerFactory;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSQLException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

@Singleton
class CtrlRscConnectionApiCallHandler
{
    private final AccessContext apiCtx;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final ResourceConnectionDataControllerFactory resourceConnectionDataFactory;
    private final CtrlSatelliteUpdater ctrlSatelliteUpdater;
    private final ResponseConverter responseConverter;
    private final Provider<Peer> peer;
    private final Provider<AccessContext> peerAccCtx;

    @Inject
    CtrlRscConnectionApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        ResourceConnectionDataControllerFactory resourceConnectionDataFactoryRef,
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
        resourceConnectionDataFactory = resourceConnectionDataFactoryRef;
        ctrlSatelliteUpdater = ctrlSatelliteUpdaterRef;
        responseConverter = responseConverterRef;
        peer = peerRef;
        peerAccCtx = peerAccCtxRef;
    }

    public ApiCallRc createResourceConnection(
        String nodeName1Str,
        String nodeName2Str,
        String rscNameStr,
        Map<String, String> rscConnPropsMap
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeResourceConnectionContext(
            ApiOperation.makeCreateOperation(),
            nodeName1Str,
            nodeName2Str,
            rscNameStr
        );

        try
        {
            ResourceConnectionData rscConn = createRscConn(nodeName1Str, nodeName2Str, rscNameStr, null);

            ctrlPropsHelper.fillProperties(
                LinStorObject.RSC_CONN, rscConnPropsMap, getProps(rscConn), ApiConsts.FAIL_ACC_DENIED_RSC_CONN);

            ctrlTransactionHelper.commit();

            responseConverter.addWithDetail(responses, context, updateSatellites(rscConn));
            responseConverter.addWithOp(responses, context, ApiSuccessUtils.defaultCreatedEntry(
                rscConn.getUuid(), getResourceConnectionDescriptionInline(apiCtx, rscConn)));
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    public ApiCallRc modifyRscConnection(
        UUID rscConnUuid,
        String nodeName1,
        String nodeName2,
        String rscNameStr,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeResourceConnectionContext(
            ApiOperation.makeModifyOperation(),
            nodeName1,
            nodeName2,
            rscNameStr
        );

        try
        {
            ResourceConnectionData rscConn = loadRscConn(nodeName1, nodeName2, rscNameStr);
            if (rscConn == null)
            {
                rscConn = createRscConn(nodeName1, nodeName2, rscNameStr, null);
            }

            if (rscConnUuid != null && !rscConnUuid.equals(rscConn.getUuid()))
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UUID_RSC_CONN,
                    "UUID-check failed"
                ));
            }

            Props props = getProps(rscConn);
            Map<String, String> propsMap = props.map();

            ctrlPropsHelper.fillProperties(
                LinStorObject.RSC_CONN, overrideProps, getProps(rscConn), ApiConsts.FAIL_ACC_DENIED_RSC_CONN);

            for (String delKey : deletePropKeys)
            {
                propsMap.remove(delKey);
            }

            ctrlTransactionHelper.commit();

            responseConverter.addWithOp(responses, context, ApiSuccessUtils.defaultModifiedEntry(
                rscConn.getUuid(), getResourceConnectionDescriptionInline(apiCtx, rscConn)));
            responseConverter.addWithDetail(responses, context, updateSatellites(rscConn));
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    public ApiCallRc deleteResourceConnection(
        String nodeName1Str,
        String nodeName2Str,
        String rscNameStr
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeResourceConnectionContext(
            ApiOperation.makeDeleteOperation(),
            nodeName1Str,
            nodeName2Str,
            rscNameStr
        );

        try
        {
            ResourceConnectionData rscConn = loadRscConn(nodeName1Str, nodeName2Str, rscNameStr);
            UUID rscConnUuid = rscConn.getUuid();
            delete(rscConn);

            ctrlTransactionHelper.commit();

            responseConverter.addWithDetail(responses, context, updateSatellites(rscConn));
            responseConverter.addWithOp(responses, context, ApiSuccessUtils.defaultDeletedEntry(
                rscConnUuid, getResourceConnectionDescriptionInline(nodeName1Str, nodeName2Str, rscNameStr)));
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    private ResourceConnectionData createRscConn(
        String nodeName1Str,
        String nodeName2Str,
        String rscNameStr,
        ResourceConnection.RscConnFlags[] initFlags
    )
    {
        NodeData node1 = ctrlApiDataLoader.loadNode(nodeName1Str, true);
        NodeData node2 = ctrlApiDataLoader.loadNode(nodeName2Str, true);
        ResourceName rscName = LinstorParsingUtils.asRscName(rscNameStr);

        Resource rsc1 = loadRsc(node1, rscName);
        Resource rsc2 = loadRsc(node2, rscName);

        ResourceConnectionData rscConn;
        try
        {
            rscConn = resourceConnectionDataFactory.create(
                peerAccCtx.get(),
                rsc1,
                rsc2,
                initFlags,
                null,
                false
            );
        }
        catch (ValueOutOfRangeException | ValueInUseException exc)
        {
            // Port cannot be specified
            throw new ImplementationError(exc);
        }
        catch (ExhaustedPoolException exc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_POOL_EXHAUSTED_TCP_PORT,
                "Could not find free TCP port"
            ), exc);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "create " + getResourceConnectionDescriptionInline(nodeName1Str, nodeName2Str, rscNameStr),
                ApiConsts.FAIL_ACC_DENIED_RSC_CONN
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_EXISTS_RSC_CONN,
                "The " + getResourceConnectionDescriptionInline(nodeName1Str, nodeName2Str, rscNameStr) +
                    " already exists."
            ), dataAlreadyExistsExc);
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
        return rscConn;
    }

    private ResourceConnectionData loadRscConn(
        String nodeName1,
        String nodeName2,
        String rscNameStr
    )
    {
        NodeData node1 = ctrlApiDataLoader.loadNode(nodeName1, true);
        NodeData node2 = ctrlApiDataLoader.loadNode(nodeName2, true);
        ResourceName rscName = LinstorParsingUtils.asRscName(rscNameStr);

        Resource rsc1 = loadRsc(node1, rscName);
        Resource rsc2 = loadRsc(node2, rscName);

        ResourceConnectionData rscConn;
        try
        {
            rscConn = ResourceConnectionData.get(
                peerAccCtx.get(),
                rsc1,
                rsc2
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "load " + getResourceConnectionDescriptionInline(nodeName1, nodeName2, rscNameStr),
                ApiConsts.FAIL_ACC_DENIED_RSC_CONN
            );
        }
        return rscConn;
    }

    private Resource loadRsc(NodeData node, ResourceName rscName)
    {
        Resource rsc;
        try
        {
            rsc = node.getResource(
                peerAccCtx.get(),
                rscName
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "load resource '" + rscName.displayValue + "' from node '" + node.getName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        return rsc;
    }

    private Props getProps(ResourceConnectionData rscConn)
    {
        Props props;
        try
        {
            props = rscConn.getProps(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access properties of " + getResourceConnectionDescriptionInline(apiCtx, rscConn),
                ApiConsts.FAIL_ACC_DENIED_RSC_CONN
            );
        }
        return props;
    }

    private ApiCallRc updateSatellites(ResourceConnectionData rscConn)
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        try
        {
            responses.addEntries(ctrlSatelliteUpdater.updateSatellites(rscConn.getSourceResource(apiCtx)));
            responses.addEntries(ctrlSatelliteUpdater.updateSatellites(rscConn.getTargetResource(apiCtx)));
        }
        catch (AccessDeniedException implErr)
        {
            throw new ImplementationError(implErr);
        }

        return responses;
    }

    private void delete(ResourceConnectionData rscConn)
    {
        try
        {
            rscConn.delete(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "delete " + getResourceConnectionDescriptionInline(apiCtx, rscConn),
                ApiConsts.FAIL_ACC_DENIED_RSC_CONN
            );
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
    }

    public static String getResourceConnectionDescription(String nodeName1, String nodeName2, String rscName)
    {
        return "Resource connection between nodes " + nodeName1 + " and " +
            nodeName2 + " for resource " + rscName;
    }

    public static String getResourceConnectionDescriptionInline(AccessContext accCtx, ResourceConnectionData rscConn)
    {
        String descriptionInline;
        try
        {
            descriptionInline = getResourceConnectionDescriptionInline(
                rscConn.getSourceResource(accCtx).getAssignedNode().getName().displayValue,
                rscConn.getTargetResource(accCtx).getAssignedNode().getName().displayValue,
                rscConn.getSourceResource(accCtx).getDefinition().getName().displayValue
            );
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return descriptionInline;
    }

    public static String getResourceConnectionDescriptionInline(String nodeName1, String nodeName2, String rscName)
    {
        return "resource connection between nodes '" + nodeName1 + "' and '" +
            nodeName2 + "' for resource '" + rscName + "'";
    }

    private static ResponseContext makeResourceConnectionContext(
        ApiOperation operation,
        String nodeName1Str,
        String nodeName2Str,
        String rscNameStr
    )
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_1ST_NODE, nodeName1Str);
        objRefs.put(ApiConsts.KEY_2ND_NODE, nodeName2Str);
        objRefs.put(ApiConsts.KEY_RSC_DFN, rscNameStr);

        return new ResponseContext(
            operation,
            getResourceConnectionDescription(nodeName1Str, nodeName2Str, rscNameStr),
            getResourceConnectionDescriptionInline(nodeName1Str, nodeName2Str, rscNameStr),
            ApiConsts.MASK_RSC_CONN,
            objRefs
        );
    }
}
