package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolData;
import com.linbit.linstor.StorPoolDataFactory;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.StorPoolDefinitionData;
import com.linbit.linstor.StorPoolDefinitionDataFactory;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.Volume;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.pojo.FreeSpacePojo;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ControllerSecurityModule;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import static java.util.stream.Collectors.toList;

class CtrlStorPoolApiCallHandler extends AbsApiCallHandler
{
    private String currentNodeNameStr;
    private String currentStorPoolNameStr;
    private final CtrlClientSerializer clientComSerializer;
    private final ObjectProtection nodesMapProt;
    private final ObjectProtection storPoolDfnMapProt;
    private final CoreModule.StorPoolDefinitionMap storPoolDfnMap;
    private final StorPoolDefinitionDataFactory storPoolDefinitionDataFactory;
    private final StorPoolDataFactory storPoolDataFactory;

    @Inject
    CtrlStorPoolApiCallHandler(
        ErrorReporter errorReporterRef,
        CtrlStltSerializer interComSerializer,
        CtrlClientSerializer clientComSerializerRef,
        @ApiContext AccessContext apiCtxRef,
        @Named(ControllerSecurityModule.NODES_MAP_PROT) ObjectProtection nodesMapProtRef,
        @Named(ControllerSecurityModule.STOR_POOL_DFN_MAP_PROT) ObjectProtection storPoolDfnMapProtRef,
        CoreModule.StorPoolDefinitionMap storPoolDfnMapRef,
        CtrlObjectFactories objectFactories,
        StorPoolDefinitionDataFactory storPoolDefinitionDataFactoryRef,
        StorPoolDataFactory storPoolDataFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        @PeerContext AccessContext peerAccCtxRef,
        Peer peerRef
    )
    {
        super(
            errorReporterRef,
            apiCtxRef,
            ApiConsts.MASK_STOR_POOL,
            interComSerializer,
            objectFactories,
            transMgrProviderRef,
            peerAccCtxRef,
            peerRef
        );

        nodesMapProt = nodesMapProtRef;
        storPoolDfnMapProt = storPoolDfnMapProtRef;
        storPoolDfnMap = storPoolDfnMapRef;

        clientComSerializer = clientComSerializerRef;
        storPoolDefinitionDataFactory = storPoolDefinitionDataFactoryRef;
        storPoolDataFactory = storPoolDataFactoryRef;
    }

    public ApiCallRc createStorPool(
        String nodeNameStr,
        String storPoolNameStr,
        String driver,
        Map<String, String> storPoolPropsMap
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        try (
            AbsApiCallHandler basicallyThis = setContext(
                ApiCallType.CREATE,
                apiCallRc,
                nodeNameStr,
                storPoolNameStr
            );
        )
        {
            // as the storage pool definition is implicitly created if it doesn't exist
            // we always will update the storPoolDfnMap even if not necessary
            // Therefore we need to be able to modify apiCtrlAccessors.storPoolDfnMap
            requireStorPoolDfnMapChangeAccess();

            StorPoolData storPool = createStorPool(nodeNameStr, storPoolNameStr, driver);
            getProps(storPool).map().putAll(storPoolPropsMap);

            commit();

            updateStorPoolDfnMap(storPool);
            updateSatellite(storPool);

            reportSuccess(storPool.getUuid());
        }
        catch (ApiCallHandlerFailedException ignore)
        {
            // a report and a corresponding api-response already created. nothing to do here
        }
        catch (Exception | ImplementationError exc)
        {
            reportStatic(
                exc,
                ApiCallType.CREATE,
                getObjectDescriptionInline(nodeNameStr, storPoolNameStr),
                getObjRefs(nodeNameStr, storPoolNameStr),
                getVariables(nodeNameStr, storPoolNameStr),
                apiCallRc
            );
        }

        return apiCallRc;
    }

    public ApiCallRc modifyStorPool(
        UUID storPoolUuid,
        String nodeNameStr,
        String storPoolNameStr,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try (
            AbsApiCallHandler basicallyThis = setContext(
                ApiCallType.MODIFY,
                apiCallRc,
                nodeNameStr,
                storPoolNameStr
            );
        )
        {
            StorPoolData storPool = loadStorPool(nodeNameStr, storPoolNameStr, true);

            if (storPoolUuid != null && !storPoolUuid.equals(storPool.getUuid()))
            {
                addAnswer(
                    "UUID-check failed",
                    ApiConsts.FAIL_UUID_STOR_POOL
                );
                throw new ApiCallHandlerFailedException();
            }

            Props props = getProps(storPool);
            Map<String, String> propsMap = props.map();

            propsMap.putAll(overrideProps);

            for (String delKey : deletePropKeys)
            {
                propsMap.remove(delKey);
            }

            commit();

            reportSuccess(storPool.getUuid());
            updateSatellite(storPool);
        }
        catch (ApiCallHandlerFailedException ignore)
        {
            // a report and a corresponding api-response already created. nothing to do here
        }
        catch (Exception | ImplementationError exc)
        {
            reportStatic(
                exc,
                ApiCallType.MODIFY,
                getObjectDescriptionInline(nodeNameStr, storPoolNameStr),
                getObjRefs(nodeNameStr, storPoolNameStr),
                getVariables(nodeNameStr, storPoolNameStr),
                apiCallRc
            );
        }

        return apiCallRc;
    }

    public ApiCallRc deleteStorPool(
        String nodeNameStr,
        String storPoolNameStr
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        try (
            AbsApiCallHandler basicallyThis = setContext(
                ApiCallType.DELETE,
                apiCallRc,
                nodeNameStr,
                storPoolNameStr
            );
        )
        {
            StorPoolData storPool = loadStorPool(nodeNameStr, storPoolNameStr, false);

            if (storPool == null)
            {
                addAnswer(
                    "Deletion of " + getObjectDescriptionInline() + " had no effect.",
                    getObjectDescriptionInlineFirstLetterCaps() + " does not exist.",
                    null,
                    null,
                    ApiConsts.WARN_NOT_FOUND
                );
                throw new ApiCallHandlerFailedException();
            }
            Collection<Volume> volumes = getVolumes(storPool);
            if (!volumes.isEmpty())
            {
                StringBuilder volListSb = new StringBuilder();
                for (Volume vol : volumes)
                {
                    volListSb.append("\n   Node name: '")
                             .append(vol.getResource().getAssignedNode().getName().displayValue)
                             .append("', resource name: '")
                             .append(vol.getResource().getDefinition().getName().displayValue)
                             .append("', volume number: ")
                             .append(vol.getVolumeDefinition().getVolumeNumber().value);
                }

                addAnswer(
                    String.format(
                        "The specified storage pool '%s' on node '%s' can not be deleted as " +
                            "volumes are still using it.",
                        storPoolNameStr,
                        nodeNameStr
                    ),
                    null,
                    "Volumes that are still using the storage pool: " + volListSb.toString(),
                    volumes.size() == 1 ?
                        "Delete the listed volume first." :
                        "Delete the listed volumes first.",
                    ApiConsts.FAIL_IN_USE
                );

                throw new ApiCallHandlerFailedException();
            }
            else
            {
                UUID storPoolUuid = storPool.getUuid(); // cache storpool uuid to avoid access deleted storpool
                delete(storPool);
                commit();

                reportSuccess(storPoolUuid);
            }
        }
        catch (ApiCallHandlerFailedException ignore)
        {
            // a report and a corresponding api-response already created. nothing to do here
        }
        catch (Exception | ImplementationError exc)
        {
            reportStatic(
                exc,
                ApiCallType.DELETE,
                getObjectDescriptionInline(nodeNameStr, storPoolNameStr),
                getObjRefs(nodeNameStr, storPoolNameStr),
                getVariables(nodeNameStr, storPoolNameStr),
                apiCallRc
            );
        }

        return apiCallRc;
    }

    public void respondStorPool(int msgId, UUID storPoolUuid, String storPoolNameStr)
    {
        try
        {
            StorPoolName storPoolName = new StorPoolName(storPoolNameStr);

            StorPool storPool = peer.getNode().getStorPool(apiCtx, storPoolName);
            // TODO: check if the storPool has the same uuid as storPoolUuid
            if (storPool != null)
            {
                long fullSyncTimestamp = peer.getFullSyncId();
                long updateId = peer.getNextSerializerId();
                peer.sendMessage(
                    internalComSerializer
                        .builder(InternalApiConsts.API_APPLY_STOR_POOL, msgId)
                        .storPoolData(storPool, fullSyncTimestamp, updateId)
                        .build()
                );
            }
            else
            {
                peer.sendMessage(
                    internalComSerializer
                        .builder(InternalApiConsts.API_APPLY_STOR_POOL_DELETED, msgId)
                        .deletedStorPoolData(storPoolNameStr)
                        .build()
                );
            }
        }
        catch (InvalidNameException invalidNameExc)
        {
            errorReporter.reportError(
                new ImplementationError(
                    "Satellite requested data for invalid storpool name '" + storPoolNameStr + "'.",
                    invalidNameExc
                )
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            errorReporter.reportError(
                new ImplementationError(
                    "Controller's api context has not enough privileges to gather requested storpool data.",
                    accDeniedExc
                )
            );
        }
    }

    byte[] listStorPools(int msgId)
    {
        ArrayList<StorPool.StorPoolApi> storPools = new ArrayList<>();
        try
        {
            nodesMapProt.requireAccess(peerAccCtx, AccessType.VIEW);
            storPoolDfnMapProt.requireAccess(peerAccCtx, AccessType.VIEW);
            for (StorPoolDefinition storPoolDfn : storPoolDfnMap.values())
            {
                try
                {
                    for (StorPool storPool : storPoolDfn.streamStorPools(peerAccCtx).collect(toList()))
                    {
                        if (!storPool.getName().getDisplayName().equals(LinStor.DISKLESS_STOR_POOL_NAME))
                        {
                            storPools.add(storPool.getApiData(peerAccCtx, null, null));
                        }
                        // fullSyncId and updateId null, as they are not going to be serialized anyways
                    }
                }
                catch (AccessDeniedException accDeniedExc)
                {
                    // don't add storpooldfn without access
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            // for now return an empty list.
        }

        return clientComSerializer
            .builder(ApiConsts.API_LST_STOR_POOL, msgId)
            .storPoolList(storPools)
            .build();
    }

    void updateRealFreeSpace(Peer peer, FreeSpacePojo[] freeSpacePojos)
    {
        try (
            AbsApiCallHandler basicallyThis = setContext(
                ApiCallType.MODIFY,
                null, // apiCallRc
                peer.getNode().getName().displayValue,
                null // storPoolName
            );
        )
        {
            String nodeName = peer.getNode().getName().displayValue;

            for (FreeSpacePojo freeSpacePojo : freeSpacePojos)
            {
                currentStorPoolNameStr = freeSpacePojo.getStorPoolName();

                StorPoolData storPool = loadStorPool(nodeName, freeSpacePojo.getStorPoolName(), true);
                if (storPool.getUuid().equals(freeSpacePojo.getStorPoolUuid()))
                {
                    setRealFreeSpace(storPool, freeSpacePojo.getFreeSpace());
                }
                else
                {
                    throw asExc(
                        null,
                        "UUIDs mismatched when updating free space of " + getObjectDescriptionInline(),
                        ApiConsts.FAIL_UUID_STOR_POOL
                    );
                }
            }

            commit();
        }
        catch (ApiCallHandlerFailedException ignored)
        {
            // already reported
        }
    }

    private void setRealFreeSpace(StorPoolData storPool, long freeSpace)
    {
        try
        {
            storPool.setRealFreeSpace(peerAccCtx, freeSpace);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
        catch (SQLException exc)
        {
            throw asImplError(exc);
        }
    }

    private AbsApiCallHandler setContext(
        ApiCallType type,
        ApiCallRcImpl apiCallRc,
        String nodeNameStr,
        String storPoolNameStr
    )
    {
        super.setContext(
            type,
            apiCallRc,
            true, // autoClose
            getObjRefs(nodeNameStr, storPoolNameStr),
            getVariables(nodeNameStr, storPoolNameStr)
        );
        currentNodeNameStr = nodeNameStr;
        currentStorPoolNameStr = storPoolNameStr;

        return this;
    }

    private void requireStorPoolDfnMapChangeAccess()
    {
        try
        {
            storPoolDfnMapProt.requireAccess(
                peerAccCtx,
                AccessType.CHANGE
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "change any storage pools.",
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL_DFN
            );
        }
    }

    private StorPoolData createStorPool(String nodeNameStr, String storPoolNameStr, String driver)
    {
        NodeData node = loadNode(nodeNameStr, true);
        StorPoolDefinitionData storPoolDef = loadStorPoolDfn(storPoolNameStr, false);

        StorPoolData storPool;
        try
        {
            if (storPoolDef == null)
            {
                // implicitly create storage pool definition if it doesn't exist
                storPoolDef = storPoolDefinitionDataFactory.getInstance(
                    peerAccCtx,
                    asStorPoolName(storPoolNameStr),
                    true,  // create and persist if not exists
                    false  // do not throw exception if exists
                );
            }

            storPool = storPoolDataFactory.getInstance(
                peerAccCtx,
                node,
                storPoolDef,
                driver,
                true,
                true
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "create " + getObjectDescriptionInline(),
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL
            );
        }
        catch (LinStorDataAlreadyExistsException alreadyExistsExc)
        {
            throw asExc(
                alreadyExistsExc,
                getObjectDescription() + " already exists.",
                ApiConsts.FAIL_EXISTS_STOR_POOL
            );
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(
                sqlExc,
                "creating " + getObjectDescriptionInline()
            );
        }
        return storPool;
    }

    private void updateStorPoolDfnMap(StorPoolData storPool)
    {
        try
        {
            storPoolDfnMap.put(
                storPool.getName(),
                storPool.getDefinition(apiCtx)
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
    }

    private Collection<Volume> getVolumes(StorPoolData storPool)
    {
        Collection<Volume> volumes;
        try
        {
            volumes = storPool.getVolumes(peerAccCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "access the volumes of " + getObjectDescriptionInline(),
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL
            );
        }
        return volumes;
    }

    private void delete(StorPoolData storPool)
    {
        try
        {
            storPool.delete(peerAccCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "delete " + getObjectDescriptionInline(),
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL
            );
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(
                sqlExc,
                "deleting " + getObjectDescriptionInline()
            );
        }
    }

    @Override
    protected String getObjectDescription()
    {
        return "Node: " + currentNodeNameStr + ", Storage pool name: " + currentStorPoolNameStr;
    }

    @Override
    protected String getObjectDescriptionInline()
    {
        return getObjectDescriptionInline(currentNodeNameStr, currentStorPoolNameStr);
    }

    public static String getObjectDescriptionInline(StorPool storPool)
    {
        return getObjectDescriptionInline(
            storPool.getNode().getName().displayValue,
            storPool.getName().displayValue
        );
    }

    public static String getObjectDescriptionInline(String nodeNameStr, String storPoolNameStr)
    {
        return "storage pool '" + storPoolNameStr + "' on node '" + nodeNameStr + "'";
    }

    private Map<String, String> getObjRefs(String nodeNameStr, String storPoolNameStr)
    {
        Map<String, String> map = new TreeMap<>();
        map.put(ApiConsts.KEY_NODE, nodeNameStr);
        map.put(ApiConsts.KEY_STOR_POOL_DFN, storPoolNameStr);
        return map;
    }

    private Map<String, String> getVariables(String nodeNameStr, String storPoolNameStr)
    {
        Map<String, String> vars = new TreeMap<>();
        vars.put(ApiConsts.KEY_NODE_NAME, nodeNameStr);
        vars.put(ApiConsts.KEY_STOR_POOL_NAME, storPoolNameStr);
        return vars;
    }

    private StorPoolData loadStorPool(String nodeNameStr, String storPoolNameStr, boolean failIfNull)
    {
        return loadStorPool(
            loadStorPoolDfn(storPoolNameStr, true),
            loadNode(nodeNameStr, true),
            failIfNull
        );
    }

    protected final Props getProps(StorPoolData storPool) throws ApiCallHandlerFailedException
    {
        Props props;
        try
        {
            props = storPool.getProps(peerAccCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "access properties of storage pool '" + storPool.getName().displayValue +
                "' on node '" + storPool.getNode().getName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL
            );
        }
        return props;
    }
}
