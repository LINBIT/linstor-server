package com.linbit.linstor.core.apicallhandler.controller.helpers;

import com.linbit.InvalidNameException;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiDataLoader;
import com.linbit.linstor.core.apicallhandler.controller.exceptions.IllegalStorageDriverException;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.objects.FreeSpaceMgrControllerFactory;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolControllerFactory;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.objects.StorPoolDefinitionControllerFactory;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Provider;

public class StorPoolHelper
{
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final StorPoolDefinitionControllerFactory storPoolDefinitionFactory;
    private final StorPoolControllerFactory storPoolFactory;
    private final Provider<AccessContext> peerAccCtx;
    private final FreeSpaceMgrControllerFactory freeSpaceMgrFactory;

    @Inject
    public StorPoolHelper(
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        StorPoolDefinitionControllerFactory storPoolDefinitionFactoryRef,
        StorPoolControllerFactory storPoolFactoryRef,
        FreeSpaceMgrControllerFactory freeSpaceMgrFactoryRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef
    )
    {
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        storPoolDefinitionFactory = storPoolDefinitionFactoryRef;
        storPoolFactory = storPoolFactoryRef;
        freeSpaceMgrFactory = freeSpaceMgrFactoryRef;
        peerAccCtx = peerAccCtxRef;
    }

    public StorPool createStorPool(
        String nodeNameStr,
        String storPoolNameStr,
        @Nonnull DeviceProviderKind deviceProviderKindRef,
        @Nullable String sharedStorPoolNameStr,
        boolean externalLockingRef
    )
    {
        Node node = ctrlApiDataLoader.loadNode(nodeNameStr, true);
        StorPoolDefinition storPoolDef = ctrlApiDataLoader.loadStorPoolDfn(storPoolNameStr, false);

        if (!isDeviceProviderKindAllowed(node, deviceProviderKindRef))
        {
            throw new ApiRcException(
                ApiCallRcImpl.entryBuilder(
                    ApiConsts.FAIL_STLT_DOES_NOT_SUPPORT_PROVIDER,
                    "The satellite does not support the device provider " + deviceProviderKindRef
                ).build()
            );
        }

        StorPool storPool;
        try
        {
            if (storPoolDef == null)
            {
                // implicitly create storage pool definition if it doesn't exist
                storPoolDef = storPoolDefinitionFactory.create(
                    peerAccCtx.get(),
                    LinstorParsingUtils.asStorPoolName(storPoolNameStr)
                );
            }

            SharedStorPoolName sharedSpaceName = sharedStorPoolNameStr != null && !sharedStorPoolNameStr.isEmpty() ?
                LinstorParsingUtils.asSharedStorPoolName(sharedStorPoolNameStr) :
                new SharedStorPoolName(node.getName(), storPoolDef.getName());

            storPool = storPoolFactory.create(
                peerAccCtx.get(),
                node,
                storPoolDef,
                deviceProviderKindRef,
                freeSpaceMgrFactory.getInstance(peerAccCtx.get(), sharedSpaceName),
                externalLockingRef
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
                .setSkipErrorReport(true)
                .build(),
                alreadyExistsExc
            );
        }
        catch (InvalidNameException invlExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_INVLD_STOR_POOL_NAME, invlExc.getMessage())
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
        Node node,
        DeviceProviderKind kind
    )
    {
        boolean isKindAllowed;
        try
        {
            // TODO try to skip creation of dfltDisklessStorPool if no DRBD is available
            Peer peer = node.getPeer(peerAccCtx.get());
            isKindAllowed = peer.getExtToolsManager().isProviderSupported(kind);
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
