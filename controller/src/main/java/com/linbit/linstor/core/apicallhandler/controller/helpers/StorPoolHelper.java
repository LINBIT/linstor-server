package com.linbit.linstor.core.apicallhandler.controller.helpers;

import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiDataLoader;
import com.linbit.linstor.core.apicallhandler.controller.exceptions.IllegalStorageDriverException;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.FreeSpaceMgrName;
import com.linbit.linstor.core.objects.FreeSpaceMgrControllerFactory;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.NodeData;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolData;
import com.linbit.linstor.core.objects.StorPoolDataControllerFactory;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.objects.StorPoolDefinitionData;
import com.linbit.linstor.core.objects.StorPoolDefinitionDataControllerFactory;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import javax.inject.Inject;
import javax.inject.Provider;

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
        DeviceProviderKind deviceProviderKindRef,
        String freeSpaceMgrNameStr
    )
    {
        NodeData node = ctrlApiDataLoader.loadNode(nodeNameStr, true);
        StorPoolDefinitionData storPoolDef = ctrlApiDataLoader.loadStorPoolDfn(storPoolNameStr, false);

        if (!isDeviceProviderKindAllowed(node, deviceProviderKindRef))
        {
            throw new ApiRcException(
                ApiCallRcImpl.entryBuilder(
                    ApiConsts.FAIL_STLT_DOES_NOT_SUPPORT_PROVIDER,
                    "The satellite does not support the device provider " + deviceProviderKindRef
                ).build()
            );
        }

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
                deviceProviderKindRef,
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
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
        catch (IllegalStorageDriverException illStorDrivExc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.copyFromLinstorExc(
                    ApiConsts.FAIL_INVLD_STOR_DRIVER,
                    illStorDrivExc
                ),
                illStorDrivExc
            );
        }
        return storPool;
    }

    private boolean isDeviceProviderKindAllowed(
        NodeData node,
        DeviceProviderKind kind
    )
    {
        boolean isKindAllowed;
        try
        {
            // TODO try to skip creation of dfltDisklessStorPool if no DRBD is available
            Peer peer = node.getPeer(peerAccCtx.get());
            isKindAllowed =
                peer == null || // if we are creating the node we also create a dfltDisklessStorPool
                // where this peer will be uninitialized
                peer.getSupportedProviders().contains(kind);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "access node " + node.getName() + "'s peer object",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        return isKindAllowed;
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

    public static String getStorPoolDescriptionInline(Node node, StorPoolDefinition storPoolDfn)
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
