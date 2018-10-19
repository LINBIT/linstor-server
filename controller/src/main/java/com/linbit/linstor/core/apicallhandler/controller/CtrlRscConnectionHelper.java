package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceConnection;
import com.linbit.linstor.ResourceConnectionData;
import com.linbit.linstor.ResourceConnectionDataControllerFactory;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSQLException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.UUID;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscConnectionApiCallHandler.getResourceConnectionDescriptionInline;

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
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
        return rscConn;
    }

    public ResourceConnectionData loadRscConn(
        String nodeName1,
        String nodeName2,
        String rscNameStr
    )
    {
        NodeData node1 = ctrlApiDataLoader.loadNode(nodeName1, true);
        NodeData node2 = ctrlApiDataLoader.loadNode(nodeName2, true);
        ResourceName rscName = LinstorParsingUtils.asRscName(rscNameStr);

        Resource rsc1 = ctrlApiDataLoader.loadRsc(node1.getName(), rscName, false);
        Resource rsc2 = ctrlApiDataLoader.loadRsc(node2.getName(), rscName, false);

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
}
