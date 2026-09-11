package com.linbit.linstor.core.apicallhandler.controller.helpers;

import com.linbit.InvalidNameException;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiDataLoader;
import com.linbit.linstor.core.apicallhandler.controller.exceptions.IllegalStorageDriverException;
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
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import jakarta.inject.Inject;

public class StorPoolHelper
{
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final StorPoolDefinitionControllerFactory storPoolDefinitionFactory;
    private final StorPoolControllerFactory storPoolFactory;
    private final FreeSpaceMgrControllerFactory freeSpaceMgrFactory;

    @Inject
    public StorPoolHelper(
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        StorPoolDefinitionControllerFactory storPoolDefinitionFactoryRef,
        StorPoolControllerFactory storPoolFactoryRef,
        FreeSpaceMgrControllerFactory freeSpaceMgrFactoryRef
    )
    {
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        storPoolDefinitionFactory = storPoolDefinitionFactoryRef;
        storPoolFactory = storPoolFactoryRef;
        freeSpaceMgrFactory = freeSpaceMgrFactoryRef;
    }

    public StorPool createStorPool(
        String nodeNameStr,
        String storPoolNameStr,
        DeviceProviderKind deviceProviderKindRef,
        @Nullable String sharedStorPoolNameStr,
        boolean externalLockingRef
    )
    {
        Node node = ctrlApiDataLoader.loadNode(nodeNameStr);
        @Nullable StorPoolDefinition storPoolDef = ctrlApiDataLoader.loadStorPoolDfnOrNull(storPoolNameStr);

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
                    LinstorParsingUtils.asStorPoolName(storPoolNameStr)
                );
            }

            SharedStorPoolName sharedSpaceName = sharedStorPoolNameStr != null && !sharedStorPoolNameStr.isEmpty() ?
                LinstorParsingUtils.asSharedStorPoolName(sharedStorPoolNameStr) :
                new SharedStorPoolName(node.getName(), storPoolDef.getName());

            storPool = storPoolFactory.create(
                node,
                storPoolDef,
                deviceProviderKindRef,
                freeSpaceMgrFactory.getInstance(sharedSpaceName),
                externalLockingRef
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
        // TODO try to skip creation of dfltDisklessStorPool if no DRBD is available
        isKindAllowed = node.getPeer().getExtToolsManager().isProviderSupported(kind);
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
