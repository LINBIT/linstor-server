package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.ResourceConnectionControllerFactory;
import com.linbit.linstor.dbdrivers.DatabaseException;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscConnectionApiCallHandler.getResourceConnectionDescriptionInline;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.UUID;

@Singleton
class CtrlRscConnectionHelper
{
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final ResourceConnectionControllerFactory resourceConnectionFactory;

    @Inject
    CtrlRscConnectionHelper(
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        ResourceConnectionControllerFactory resourceConnectionFactoryRef
    )
    {
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        resourceConnectionFactory = resourceConnectionFactoryRef;
    }

    public ResourceConnection loadOrCreateRscConn(
        @Nullable UUID rscConnUuid,
        String nodeName1,
        String nodeName2,
        String rscNameStr
    )
    {
        @Nullable ResourceConnection rscConn = loadRscConnOrNull(nodeName1, nodeName2, rscNameStr);
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

    public ResourceConnection createRscConn(
        String nodeName1Str,
        String nodeName2Str,
        String rscNameStr,
        @Nullable ResourceConnection.Flags[] initFlags
    )
    {
        Node node1 = ctrlApiDataLoader.loadNode(nodeName1Str);
        Node node2 = ctrlApiDataLoader.loadNode(nodeName2Str);
        ResourceName rscName = LinstorParsingUtils.asRscName(rscNameStr);

        Resource rsc1 = ctrlApiDataLoader.loadRsc(node1.getName(), rscName);
        Resource rsc2 = ctrlApiDataLoader.loadRsc(node2.getName(), rscName);

        return createRscConn(rsc1, rsc2, initFlags);
    }

    public ResourceConnection createRscConn(
        Resource rsc1,
        Resource rsc2,
        @Nullable ResourceConnection.Flags[] initFlags
    )
    {
        ResourceConnection rscConn;
        try
        {
            rscConn = resourceConnectionFactory.create(
                rsc1,
                rsc2,
                initFlags
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_EXISTS_RSC_CONN,
                "The " + getResourceConnectionDescriptionInline(
                    rsc1.getNode().getName().displayValue,
                    rsc2.getNode().getName().displayValue,
                    rsc1.getResourceDefinition().getName().displayValue
                ) + " already exists.",
                true
            ), dataAlreadyExistsExc);
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
        return rscConn;
    }

    public ResourceConnection loadRscConn(
        String nodeName1Str,
        String nodeName2Str,
        String rscNameStr
    )
    {
        ResourceName rscName = LinstorParsingUtils.asRscName(rscNameStr);
        NodeName nodeName1 = LinstorParsingUtils.asNodeName(nodeName1Str);
        NodeName nodeName2 = LinstorParsingUtils.asNodeName(nodeName2Str);

        Resource rsc1 = ctrlApiDataLoader.loadRsc(nodeName1, rscName);
        Resource rsc2 = ctrlApiDataLoader.loadRsc(nodeName2, rscName);

        @Nullable ResourceConnection rscConn = rsc1.getAbsResourceConnection(rsc2);

        if (rscConn == null)
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
        return rscConn;
    }

    public @Nullable ResourceConnection loadRscConnOrNull(
        String nodeName1Str,
        String nodeName2Str,
        String rscNameStr
    )
    {
        ResourceName rscName = LinstorParsingUtils.asRscName(rscNameStr);
        NodeName nodeName1 = LinstorParsingUtils.asNodeName(nodeName1Str);
        NodeName nodeName2 = LinstorParsingUtils.asNodeName(nodeName2Str);

        @Nullable Resource rsc1 = ctrlApiDataLoader.loadRscOrNull(nodeName1, rscName);
        @Nullable Resource rsc2 = ctrlApiDataLoader.loadRscOrNull(nodeName2, rscName);

        @Nullable ResourceConnection rscConn = null;
        if (rsc1 != null && rsc2 != null)
        {
            rscConn = rsc1.getAbsResourceConnection(rsc2);
        }
        return rscConn;
    }
}
