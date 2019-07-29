package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceConnection;
import com.linbit.linstor.ResourceConnectionData;
import com.linbit.linstor.ResourceConnectionDataControllerFactory;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscConnectionApiCallHandler.getResourceConnectionDescriptionInline;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.UUID;

@Singleton
class CtrlRscConnectionHelper
{
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final ResourceConnectionDataControllerFactory resourceConnectionDataFactory;
    private final Provider<AccessContext> peerAccCtx;

    @Inject
    CtrlRscConnectionHelper(
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        ResourceConnectionDataControllerFactory resourceConnectionDataFactoryRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef
    )
    {
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        resourceConnectionDataFactory = resourceConnectionDataFactoryRef;
        peerAccCtx = peerAccCtxRef;
    }

    public ResourceConnectionData loadOrCreateRscConn(
        UUID rscConnUuid,
        String nodeName1,
        String nodeName2,
        String rscNameStr
    )
    {
        ResourceConnectionData rscConn = loadRscConn(nodeName1, nodeName2, rscNameStr, false);
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

        return rscConn;
    }

    public ResourceConnectionData createRscConn(
        String nodeName1Str,
        String nodeName2Str,
        String rscNameStr,
        ResourceConnection.RscConnFlags[] initFlags
    )
    {
        NodeData node1 = ctrlApiDataLoader.loadNode(nodeName1Str, true);
        NodeData node2 = ctrlApiDataLoader.loadNode(nodeName2Str, true);
        ResourceName rscName = LinstorParsingUtils.asRscName(rscNameStr);

        Resource rsc1 = ctrlApiDataLoader.loadRsc(node1.getName(), rscName, false);
        Resource rsc2 = ctrlApiDataLoader.loadRsc(node2.getName(), rscName, false);

        ResourceConnectionData rscConn;
        try
        {
            rscConn = resourceConnectionDataFactory.create(
                peerAccCtx.get(),
                rsc1,
                rsc2,
                initFlags
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
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
        return rscConn;
    }

    public ResourceConnectionData loadRscConn(
        String nodeName1Str,
        String nodeName2Str,
        String rscNameStr,
        boolean failIfNull
    )
    {
        ResourceName rscName = LinstorParsingUtils.asRscName(rscNameStr);
        NodeName nodeName1 = LinstorParsingUtils.asNodeName(nodeName1Str);
        NodeName nodeName2 = LinstorParsingUtils.asNodeName(nodeName2Str);

        Resource rsc1 = ctrlApiDataLoader.loadRsc(nodeName1, rscName, true);
        Resource rsc2 = ctrlApiDataLoader.loadRsc(nodeName2, rscName, true);

        ResourceConnectionData rscConn;
        try
        {
            rscConn = ResourceConnectionData.get(
                peerAccCtx.get(),
                rsc1,
                rsc2
            );

            if (failIfNull && rscConn == null)
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_NOT_FOUND_RSC_CONN,
                        String.format("Resource connection between node '%s' and '%s' not found for resource '%s'.",
                            nodeName1Str,
                            nodeName2Str,
                            rscNameStr
                        )
                    )
                );
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "load " + getResourceConnectionDescriptionInline(nodeName1Str, nodeName2Str, rscNameStr),
                ApiConsts.FAIL_ACC_DENIED_RSC_CONN
            );
        }
        return rscConn;
    }
}
