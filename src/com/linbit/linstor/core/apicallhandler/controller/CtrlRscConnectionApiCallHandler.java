package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceConnectionData;
import com.linbit.linstor.ResourceConnectionDataFactory;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.core.CtrlObjectFactories;
import com.linbit.linstor.core.apicallhandler.AbsApiCallHandler;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSQLException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

@Singleton
class CtrlRscConnectionApiCallHandler extends AbsApiCallHandler
{
    private final CtrlSatelliteUpdater ctrlSatelliteUpdater;
    private final ResourceConnectionDataFactory resourceConnectionDataFactory;
    private final ResponseConverter responseConverter;

    @Inject
    CtrlRscConnectionApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        CtrlObjectFactories objectFactories,
        ResourceConnectionDataFactory resourceConnectionDataFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        Provider<Peer> peerRef,
        WhitelistProps whitelistPropsRef,
        CtrlSatelliteUpdater ctrlSatelliteUpdaterRef,
        ResponseConverter responseConverterRef
    )
    {
        super(
            errorReporterRef,
            apiCtxRef,
            objectFactories,
            transMgrProviderRef,
            peerAccCtxRef,
            peerRef,
            whitelistPropsRef
        );
        ctrlSatelliteUpdater = ctrlSatelliteUpdaterRef;
        resourceConnectionDataFactory = resourceConnectionDataFactoryRef;
        responseConverter = responseConverterRef;
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
            peer.get(),
            ApiOperation.makeCreateOperation(),
            nodeName1Str,
            nodeName2Str,
            rscNameStr
        );

        try
        {
            ResourceConnectionData rscConn = createRscConn(nodeName1Str, nodeName2Str, rscNameStr);

            fillProperties(
                LinStorObject.RSC_CONN, rscConnPropsMap, getProps(rscConn), ApiConsts.FAIL_ACC_DENIED_RSC_CONN);

            commit();

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
            peer.get(),
            ApiOperation.makeModifyOperation(),
            nodeName1,
            nodeName2,
            rscNameStr
        );

        try
        {
            ResourceConnectionData rscConn = loadRscConn(nodeName1, nodeName2, rscNameStr, true);

            if (rscConnUuid != null && !rscConnUuid.equals(rscConn.getUuid()))
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UUID_RSC_CONN,
                    "UUID-check failed"
                ));
            }

            Props props = getProps(rscConn);
            Map<String, String> propsMap = props.map();

            fillProperties(
                LinStorObject.RSC_CONN, overrideProps, getProps(rscConn), ApiConsts.FAIL_ACC_DENIED_RSC_CONN);

            for (String delKey : deletePropKeys)
            {
                propsMap.remove(delKey);
            }

            commit();

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
            peer.get(),
            ApiOperation.makeDeleteOperation(),
            nodeName1Str,
            nodeName2Str,
            rscNameStr
        );

        try
        {
            ResourceConnectionData rscConn = loadRscConn(nodeName1Str, nodeName2Str, rscNameStr, false);
            UUID rscConnUuid = rscConn.getUuid();
            delete(rscConn);

            commit();

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
        String rscNameStr
    )
    {
        NodeData node1 = loadNode(nodeName1Str, true);
        NodeData node2 = loadNode(nodeName2Str, true);
        ResourceName rscName = LinstorParsingUtils.asRscName(rscNameStr);

        Resource rsc1 = loadRsc(node1, rscName);
        Resource rsc2 = loadRsc(node2, rscName);

        ResourceConnectionData rscConn;
        try
        {
            rscConn = resourceConnectionDataFactory.getInstance(
                peerAccCtx.get(),
                rsc1,
                rsc2,
                true,
                true
            );
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
        String rscNameStr,
        boolean createIfNotExists
    )
    {
        NodeData node1 = loadNode(nodeName1, true);
        NodeData node2 = loadNode(nodeName2, true);
        ResourceName rscName = LinstorParsingUtils.asRscName(rscNameStr);

        Resource rsc1 = loadRsc(node1, rscName);
        Resource rsc2 = loadRsc(node2, rscName);

        ResourceConnectionData rscConn;
        try
        {
            rscConn = resourceConnectionDataFactory.getInstance(
                peerAccCtx.get(),
                rsc1,
                rsc2,
                createIfNotExists,
                false
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
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ImplementationError(dataAlreadyExistsExc);
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
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
        Peer peer,
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
            peer,
            operation,
            getResourceConnectionDescription(nodeName1Str, nodeName2Str, rscNameStr),
            getResourceConnectionDescriptionInline(nodeName1Str, nodeName2Str, rscNameStr),
            ApiConsts.MASK_RSC_CONN,
            objRefs
        );
    }
}
