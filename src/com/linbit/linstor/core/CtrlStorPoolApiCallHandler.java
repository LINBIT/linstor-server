package com.linbit.linstor.core;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolData;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.StorPoolDefinitionData;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.Volume;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.netcom.IllegalMessageStateException;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;

class CtrlStorPoolApiCallHandler extends AbsApiCallHandler
{
    private final ThreadLocal<String> currentNodeNameStr = new ThreadLocal<>();
    private final ThreadLocal<String> currentStorPoolNameStr = new ThreadLocal<>();
    private final CtrlClientSerializer clientComSerializer;

    CtrlStorPoolApiCallHandler(
        ApiCtrlAccessors apiCtrlAccessorsRef,
        CtrlStltSerializer interComSerializer,
        CtrlClientSerializer clientComSerializerRef,
        AccessContext apiCtxRef
    )
    {
        super (
            apiCtrlAccessorsRef,
            apiCtxRef,
            ApiConsts.MASK_STOR_POOL,
            interComSerializer
        );
        this.clientComSerializer = clientComSerializerRef;
        super.setNullOnAutoClose(
            currentNodeNameStr,
            currentStorPoolNameStr
        );

        super.setNullOnAutoClose(currentNodeNameStr, currentStorPoolNameStr);
    }

    public ApiCallRc createStorPool(
        AccessContext accCtx,
        Peer client,
        String nodeNameStr,
        String storPoolNameStr,
        String driver,
        Map<String, String> storPoolPropsMap
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        try (
            AbsApiCallHandler basicallyThis = setContext(
                accCtx,
                client,
                ApiCallType.CREATE,
                apiCallRc,
                null, // create new transMgr
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
                apiCallRc,
                accCtx,
                client
            );
        }

        return apiCallRc;
    }

    public ApiCallRc modifyStorPool(
        AccessContext accCtx,
        Peer client,
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
                accCtx,
                client,
                ApiCallType.MODIFY,
                apiCallRc,
                null, // new transMgr
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
                apiCallRc,
                accCtx,
                client
            );
        }

        return apiCallRc;
    }

    public ApiCallRc deleteStorPool(
        AccessContext accCtx,
        Peer client,
        String nodeNameStr,
        String storPoolNameStr
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        try(
            AbsApiCallHandler basicallyThis = setContext(
                accCtx,
                client,
                ApiCallType.DELETE,
                apiCallRc,
                null, // create new transMgr
                nodeNameStr,
                storPoolNameStr
            );
        )
        {
            StorPoolData storPool = loadStorPool(nodeNameStr, storPoolNameStr, false);

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
                ApiCallType.MODIFY,
                getObjectDescriptionInline(nodeNameStr, storPoolNameStr),
                getObjRefs(nodeNameStr, storPoolNameStr),
                getVariables(nodeNameStr, storPoolNameStr),
                apiCallRc,
                accCtx,
                client
            );
        }

        return apiCallRc;
    }

    public void respondStorPool(int msgId, Peer satellitePeer, UUID storPoolUuid, String storPoolNameStr)
    {
        try
        {
            StorPoolName storPoolName = new StorPoolName(storPoolNameStr);

            StorPool storPool = satellitePeer.getNode().getStorPool(apiCtx, storPoolName);
            // TODO: check if the storPool has the same uuid as storPoolUuid
            if (storPool != null)
            {
                byte[] data = internalComSerializer
                    .builder(InternalApiConsts.API_APPLY_STOR_POOL, msgId)
                    .storPoolData(storPool)
                    .build();

                Message response = satellitePeer.createMessage();
                response.setData(data);
                satellitePeer.sendMessage(response);
            }
            else
            {
                apiCtrlAccessors.getErrorReporter().reportError(
                    new ImplementationError(
                        String.format(
                            "A requested storpool name '%s' with the uuid '%s' was not found "+
                                "in the controllers list of stor pools",
                                storPoolName,
                                storPoolUuid.toString()
                            ),
                        null
                    )
                );
            }
        }
        catch (InvalidNameException invalidNameExc)
        {
            apiCtrlAccessors.getErrorReporter().reportError(
                new ImplementationError(
                    "Satellite requested data for invalid storpool name '" + storPoolNameStr + "'.",
                    invalidNameExc
                )
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            apiCtrlAccessors.getErrorReporter().reportError(
                new ImplementationError(
                    "Controller's api context has not enough privileges to gather requested storpool data.",
                    accDeniedExc
                )
            );
        }
        catch (IllegalMessageStateException illegalMessageStateExc)
        {
            apiCtrlAccessors.getErrorReporter().reportError(
                new ImplementationError(
                    "Failed to respond to storpool data request",
                    illegalMessageStateExc
                )
            );
        }
    }

    byte[] listStorPools(int msgId, AccessContext accCtx, Peer client)
    {
        ArrayList<StorPool.StorPoolApi> storPools = new ArrayList<>();
        try {
            apiCtrlAccessors.getNodesMapProtection().requireAccess(accCtx, AccessType.VIEW);
            apiCtrlAccessors.getStorPoolDfnMapProtection().requireAccess(accCtx, AccessType.VIEW);// accDeniedExc1
            for (StorPoolDefinition storPoolDfn : apiCtrlAccessors.getStorPoolDfnMap().values())
            {
                try
                {
                    Iterator<StorPool> storPoolIterator = storPoolDfn.iterateStorPools(accCtx);
                    while (storPoolIterator.hasNext())
                    {
                        StorPool storPool = storPoolIterator.next();
                        storPools.add(storPool.getApiData(accCtx));
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

    private AbsApiCallHandler setContext(
        AccessContext accCtx,
        Peer peer,
        ApiCallType type,
        ApiCallRcImpl apiCallRc,
        TransactionMgr transMgr,
        String nodeNameStr,
        String storPoolNameStr
    )
    {
        super.setContext(
            accCtx,
            peer,
            type,
            apiCallRc,
            transMgr,
            getObjRefs(nodeNameStr, storPoolNameStr),
            getVariables(nodeNameStr, storPoolNameStr)
        );
        currentNodeNameStr.set(nodeNameStr);
        currentStorPoolNameStr.set(storPoolNameStr);

        return this;
    }

    private void requireStorPoolDfnMapChangeAccess()
    {
        try
        {
            apiCtrlAccessors.getStorPoolDfnMapProtection().requireAccess(
                currentAccCtx.get(),
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

    private StorPoolData createStorPool (String nodeNameStr, String storPoolNameStr, String driver)
    {
        NodeData node = loadNode(nodeNameStr, true);
        StorPoolDefinitionData storPoolDef = loadStorPoolDfn(storPoolNameStr, false);

        try
        {
            if (storPoolDef == null)
            {
                // implicitly create storage pool definition if it doesn't exist
                storPoolDef = StorPoolDefinitionData.getInstance( // accDeniedExc2, dataAlreadyExistsExc0
                    currentAccCtx.get(),
                    asStorPoolName(storPoolNameStr),
                    currentTransMgr.get(),
                    true,  // create and persist if not exists
                    false  // do not throw exception if exists
                );
            }

            return StorPoolData.getInstance(
                currentAccCtx.get(),
                node,
                storPoolDef,
                driver,
                currentTransMgr.get(),
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
    }

    private void updateStorPoolDfnMap(StorPoolData storPool)
    {
        try
        {
            apiCtrlAccessors.getStorPoolDfnMap().put(
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
        try
        {
            return storPool.getVolumes(currentAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "access the volumes of " + getObjectDescriptionInline(),
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL
            );
        }
    }

    private void delete(StorPoolData storPool)
    {
        try
        {
            storPool.delete(currentAccCtx.get());
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
        return "Node: " + currentNodeNameStr.get() + ", Storage pool name: " +currentStorPoolNameStr.get();
    }

    @Override
    protected String getObjectDescriptionInline()
    {
        return getObjectDescriptionInline(currentNodeNameStr.get(), currentStorPoolNameStr.get());
    }

    private String getObjectDescriptionInline(String nodeNameStr, String storPoolNameStr)
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
        try
        {
            return storPool.getProps(currentAccCtx.get());
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
    }
}
