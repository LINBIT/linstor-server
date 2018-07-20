package com.linbit.linstor.core.apicallhandler;

import javax.inject.Provider;
import java.sql.SQLException;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceData;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.SnapshotDefinitionData;
import com.linbit.linstor.SnapshotName;
import com.linbit.linstor.StorPoolData;
import com.linbit.linstor.StorPoolDefinitionData;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.CtrlObjectFactories;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSQLException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionMgr;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.getRscDfnDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlStorPoolApiCallHandler.getStorPoolDescriptionInline;

public abstract class AbsApiCallHandler
{
    public enum LinStorObject
    {
        NODE,
        NET_IF,
        NODE_CONN,
        RESOURCE_DEFINITION,
        RESOURCE,
        RSC_CONN,
        VOLUME_DEFINITION,
        VOLUME,
        VOLUME_CONN,
        CONTROLLER,
        STORAGEPOOL,
        STORAGEPOOL_DEFINITION,
        SNAPSHOT;

        LinStorObject()
        {
        }
    }

    protected final ErrorReporter errorReporter;
    protected final AccessContext apiCtx;
    private final CtrlObjectFactories objectFactories;

    private final Provider<TransactionMgr> transMgrProvider;

    protected final Provider<AccessContext> peerAccCtx;
    protected final Provider<Peer> peer;

    protected AbsApiCallHandler(
        ErrorReporter errorReporterRef,
        AccessContext apiCtxRef,
        CtrlObjectFactories objectFactoriesRef,
        Provider<TransactionMgr> transMgrProviderRef,
        Provider<AccessContext> peerAccCtxRef,
        Provider<Peer> peerRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        objectFactories = objectFactoriesRef;
        transMgrProvider = transMgrProviderRef;
        peerAccCtx = peerAccCtxRef;
        peer = peerRef;
    }

    protected final NodeData loadNode(String nodeNameStr, boolean failIfNull)
    {
        return loadNode(LinstorParsingUtils.asNodeName(nodeNameStr), failIfNull);
    }

    protected final NodeData loadNode(NodeName nodeName, boolean failIfNull)
    {
        NodeData node;
        try
        {
            node = objectFactories.getNodeDataFactory().getInstance(
                peerAccCtx.get(),
                nodeName,
                null,
                null,
                false,
                false
            );

            if (failIfNull && node == null)
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_NOT_FOUND_NODE,
                        "Node '" + nodeName.displayValue + "' not found."
                    )
                    .setCause("The specified node '" + nodeName.displayValue + "' could not be found in the database")
                    .setCorrection("Create a node with the name '" + nodeName.displayValue + "' first.")
                    .build()
                );
            }
        }
        catch (AccessDeniedException accDenied)
        {
            throw new ApiAccessDeniedException(
                accDenied,
                "loading node '" + nodeName.displayValue + "'.",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        catch (LinStorDataAlreadyExistsException alreadyExists)
        {
            throw new ImplementationError(alreadyExists);
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
        return node;
    }

    protected final ResourceDefinitionData loadRscDfn(
        String rscNameStr,
        boolean failIfNull
    )
    {
        return loadRscDfn(LinstorParsingUtils.asRscName(rscNameStr), failIfNull);
    }

    protected final ResourceDefinitionData loadRscDfn(
        ResourceName rscName,
        boolean failIfNull
    )
    {
        ResourceDefinitionData rscDfn;
        try
        {
            rscDfn = objectFactories.getResourceDefinitionDataFactory().load(
                peerAccCtx.get(),
                rscName
            );

            if (failIfNull && rscDfn == null)
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_NOT_FOUND_RSC_DFN,
                        "Resource definition '" + rscName.displayValue + "' not found."
                    )
                    .setCause("The specified resource definition '" + rscName.displayValue +
                        "' could not be found in the database")
                    .setCorrection("Create a resource definition with the name '" + rscName.displayValue + "' first.")
                    .build()
                );
            }

        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access " + getRscDfnDescriptionInline(rscName.displayValue),
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        return rscDfn;
    }

    protected ResourceData loadRsc(String nodeName, String rscName, boolean failIfNull)
    {
        Node node = loadNode(nodeName, true);
        ResourceDefinitionData rscDfn = loadRscDfn(rscName, true);

        ResourceData rscData;
        try
        {
            rscData = objectFactories.getResourceDataFactory().getInstance(
                peerAccCtx.get(),
                rscDfn,
                node,
                null,
                null,
                false,
                false
            );
            if (rscData == null && failIfNull)
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_NOT_FOUND_RSC_DFN,
                        "Resource '" + rscName + "' on node '" + nodeName + "' not found."
                    )
                    .setCause("The specified resource '" + rscName + "' on node '" + nodeName + "' could not " +
                        "be found in the database")
                    .setCorrection("Create a resource with the name '" + rscName + "' on node '" + nodeName +
                        "' first.")
                    .build()
                );
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "loading resource '" + rscName + "' on node '" + nodeName + "'",
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ImplementationError(
                "Loading a resource caused DataAlreadyExistsException",
                dataAlreadyExistsExc
            );
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
        return rscData;
    }

    protected final SnapshotDefinitionData loadSnapshotDfn(
        ResourceDefinition rscDfn,
        SnapshotName snapshotName
    )
    {
        SnapshotDefinitionData snapshotDfn;
        try
        {
            snapshotDfn = objectFactories.getSnapshotDefinitionDataFactory().load(peerAccCtx.get(), rscDfn, snapshotName);

            if (snapshotDfn == null)
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_NOT_FOUND_SNAPSHOT_DFN,
                        "Snapshot '" + snapshotName.displayValue +
                            "' of resource '" + rscDfn.getName().displayValue + "' not found."
                    )
                    .build()
                );
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "loading snapshot '" + snapshotName + "' of resource '" + rscDfn.getName() + "'",
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT_DFN
            );
        }
        return snapshotDfn;
    }

    protected final StorPoolDefinitionData loadStorPoolDfn(String storPoolNameStr, boolean failIfNull)
    {
        return loadStorPoolDfn(LinstorParsingUtils.asStorPoolName(storPoolNameStr), failIfNull);
    }

    protected final StorPoolDefinitionData loadStorPoolDfn(
        StorPoolName storPoolName,
        boolean failIfNull
    )
    {
        StorPoolDefinitionData storPoolDfn;
        try
        {
            storPoolDfn = objectFactories.getStorPoolDefinitionDataFactory().getInstance(
                peerAccCtx.get(),
                storPoolName,
                false,
                false
            );

            if (failIfNull && storPoolDfn == null)
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_NOT_FOUND_STOR_POOL_DFN,
                        "Storage pool definition '" + storPoolName.displayValue + "' not found."
                    )
                    .setCause("The specified storage pool definition '" + storPoolName.displayValue +
                        "' could not be found in the database")
                    .setCorrection("Create a storage pool definition '" + storPoolName.displayValue + "' first.")
                    .build()
                );
            }

        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "loading storage pool definition '" + storPoolName.displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL_DFN
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ImplementationError(
                "Loading storage pool caused dataAlreadyExists exception",
                dataAlreadyExistsExc
            );
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
        return storPoolDfn;
    }

    protected final StorPoolData loadStorPool(
        StorPoolDefinitionData storPoolDfn,
        NodeData node,
        boolean failIfNull
    )
    {
        StorPoolData storPool;
        try
        {
            storPool = objectFactories.getStorPoolDataFactory().getInstance(
                peerAccCtx.get(),
                node,
                storPoolDfn,
                null, // storDriverSimpleClassName
                false,
                false
            );

            if (failIfNull && storPool == null)
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_NOT_FOUND_STOR_POOL_DFN,
                        "Storage pool '" + storPoolDfn.getName().displayValue + "' on node '" +
                            node.getName().displayValue + "' not found.")
                    .setCause("The specified storage pool '" + storPoolDfn.getName().displayValue +
                        "' on node '" + node.getName().displayValue + "' could not be found in the database")
                    .setCorrection("Create a storage pool '" + storPoolDfn.getName().displayValue + "' on node '" +
                        node.getName().displayValue + "' first.")
                    .build()
                );
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "load " + getStorPoolDescriptionInline(node, storPoolDfn),
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ImplementationError(
                "Loading storage pool caused dataAlreadyExists exception",
                dataAlreadyExistsExc
            );
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
        return storPool;
    }

    protected final void commit()
    {
        try
        {
            transMgrProvider.get().commit();
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
    }
}
