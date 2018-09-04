package com.linbit.linstor.core.apicallhandler.controller.helpers;

import com.linbit.linstor.FreeSpaceMgrControllerFactory;
import com.linbit.linstor.FreeSpaceMgrName;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolData;
import com.linbit.linstor.StorPoolDataControllerFactory;
import com.linbit.linstor.StorPoolDefinitionData;
import com.linbit.linstor.StorPoolDefinitionDataControllerFactory;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiDataLoader;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSQLException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Provider;

import java.sql.SQLException;

public class StorPoolHelper
{
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final StorPoolDefinitionDataControllerFactory storPoolDefinitionDataFactory;
    private final StorPoolDataControllerFactory storPoolDataFactory;
    private final Provider<AccessContext> peerAccCtx;
    private final FreeSpaceMgrControllerFactory freeSpaceMgrFactory;

    @Inject
    public StorPoolHelper(
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        StorPoolDefinitionDataControllerFactory storPoolDefinitionDataFactoryRef,
        StorPoolDataControllerFactory storPoolDataFactoryRef,
        FreeSpaceMgrControllerFactory freeSpaceMgrFactoryRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef
    )
    {
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        storPoolDefinitionDataFactory = storPoolDefinitionDataFactoryRef;
        storPoolDataFactory = storPoolDataFactoryRef;
        freeSpaceMgrFactory = freeSpaceMgrFactoryRef;
        peerAccCtx = peerAccCtxRef;
    }

    public StorPoolData createStorPool(
        String nodeNameStr,
        String storPoolNameStr,
        String driver,
        String freeSpaceMgrNameStr
    )
    {
        NodeData node = ctrlApiDataLoader.loadNode(nodeNameStr, true);
        StorPoolDefinitionData storPoolDef = ctrlApiDataLoader.loadStorPoolDfn(storPoolNameStr, false);

        StorPoolData storPool;
        try
        {
            if (storPoolDef == null)
            {
                // implicitly create storage pool definition if it doesn't exist
                storPoolDef = storPoolDefinitionDataFactory.create(
                    peerAccCtx.get(),
                    LinstorParsingUtils.asStorPoolName(storPoolNameStr)
                );
            }

            FreeSpaceMgrName fsmName = freeSpaceMgrNameStr != null && !freeSpaceMgrNameStr.isEmpty() ?
                LinstorParsingUtils.asFreeSpaceMgrName(freeSpaceMgrNameStr) :
                new FreeSpaceMgrName(node.getName(), storPoolDef.getName());

            storPool = storPoolDataFactory.create(
                peerAccCtx.get(),
                node,
                storPoolDef,
                driver,
                freeSpaceMgrFactory.getInstance(peerAccCtx.get(), fsmName)
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "register " + getStorPoolDescriptionInline(nodeNameStr, storPoolNameStr),
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL
            );
        }
        catch (LinStorDataAlreadyExistsException alreadyExistsExc)
        {
            throw new ApiRcException(ApiCallRcImpl
                .entryBuilder(
                    ApiConsts.FAIL_EXISTS_STOR_POOL,
                    getStorPoolDescription(nodeNameStr, storPoolNameStr) + " already exists."
                )
                .build(),
                alreadyExistsExc
            );
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
        return storPool;
    }

    public static String getStorPoolDescription(String nodeNameStr, String storPoolNameStr)
    {
        return "Node: " + nodeNameStr + ", Storage pool name: " + storPoolNameStr;
    }

    public static String getStorPoolDescriptionInline(StorPool storPool)
    {
        return getStorPoolDescriptionInline(
            storPool.getNode().getName().displayValue,
            storPool.getName().displayValue
        );
    }

    public static String getStorPoolDescriptionInline(NodeData node, StorPoolDefinitionData storPoolDfn)
    {
        return getStorPoolDescriptionInline(
            node.getName().displayValue,
            storPoolDfn.getName().displayValue
        );
    }

    public static String getStorPoolDescriptionInline(String nodeNameStr, String storPoolNameStr)
    {
        return "storage pool '" + storPoolNameStr + "' on node '" + nodeNameStr + "'";
    }
}
