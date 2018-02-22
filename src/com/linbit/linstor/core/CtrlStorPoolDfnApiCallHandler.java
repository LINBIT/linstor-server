package com.linbit.linstor.core;


import com.linbit.ImplementationError;
import com.linbit.TransactionMgr;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.StorPoolDefinitionData;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ControllerSecurityModule;
import com.linbit.linstor.security.ObjectProtection;

import javax.inject.Inject;
import javax.inject.Named;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

class CtrlStorPoolDfnApiCallHandler extends AbsApiCallHandler
{
    private final ThreadLocal<String> currentStorPoolNameStr = new ThreadLocal<>();
    private final CtrlClientSerializer clientComSerializer;
    private final CoreModule.StorPoolDefinitionMap storPoolDfnMap;
    private final ObjectProtection storPoolDfnMapProt;

    @Inject
    CtrlStorPoolDfnApiCallHandler(
        ErrorReporter errorReporterRef,
        DbConnectionPool dbConnectionPoolRef,
        CtrlStltSerializer interComSerializer,
        CtrlClientSerializer clientComSerializerRef,
        @ApiContext AccessContext apiCtxRef,
        CoreModule.StorPoolDefinitionMap storPoolDfnMapRef,
        @Named(ControllerSecurityModule.STOR_POOL_DFN_MAP_PROT) ObjectProtection storPoolDfnMapProtRef
    )
    {
        super(
            errorReporterRef,
            dbConnectionPoolRef,
            apiCtxRef,
            ApiConsts.MASK_STOR_POOL_DFN,
            interComSerializer
        );
        super.setNullOnAutoClose(currentStorPoolNameStr);
        clientComSerializer = clientComSerializerRef;
        storPoolDfnMap = storPoolDfnMapRef;
        storPoolDfnMapProt = storPoolDfnMapProtRef;
    }

    public ApiCallRc createStorPoolDfn(
        AccessContext accCtx,
        Peer client,
        String storPoolNameStr,
        Map<String, String> storPoolDfnProps
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
                storPoolNameStr
            );
        )
        {
            requireStorPoolDfnChangeAccess();

            StorPoolDefinitionData storPoolDfn = createStorPool(storPoolNameStr);
            getProps(storPoolDfn).map().putAll(storPoolDfnProps);
            commit();

            storPoolDfnMap.put(storPoolDfn.getName(), storPoolDfn);
            reportSuccess(storPoolDfn.getUuid());
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
                getObjectDescriptionInline(storPoolNameStr),
                getObjRefs(storPoolNameStr),
                getVariables(storPoolNameStr),
                apiCallRc,
                accCtx,
                client
            );
        }

        return apiCallRc;
    }

    public ApiCallRc modifyStorPoolDfn(
        AccessContext accCtx,
        Peer client,
        UUID storPoolDfnUuid,
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
                null,
                storPoolNameStr
            );
        )
        {
            requireStorPoolDfnChangeAccess();
            StorPoolDefinitionData storPoolDfn = loadStorPoolDfn(storPoolNameStr, true);

            if (storPoolDfnUuid != null && !storPoolDfnUuid.equals(storPoolDfn.getUuid()))
            {
                addAnswer(
                    "UUID-check failed",
                    ApiConsts.FAIL_UUID_STOR_POOL_DFN
                );
                throw new ApiCallHandlerFailedException();
            }

            Props props = getProps(storPoolDfn);
            Map<String, String> propsMap = props.map();

            propsMap.putAll(overrideProps);

            for (String delKey : deletePropKeys)
            {
                propsMap.remove(delKey);
            }

            commit();

            updateSatellites(storPoolDfn);
            reportSuccess(storPoolDfn.getUuid());
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
                getObjectDescriptionInline(storPoolNameStr),
                getObjRefs(storPoolNameStr),
                getVariables(storPoolNameStr),
                apiCallRc,
                accCtx,
                client
            );
        }

        return apiCallRc;
    }

    public ApiCallRc deleteStorPoolDfn(AccessContext accCtx, Peer client, String storPoolNameStr)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        try (
            AbsApiCallHandler basicallyThis = setContext(
                accCtx,
                client,
                ApiCallType.DELETE,
                apiCallRc,
                null, // create new transMgr
                storPoolNameStr
            );
        )
        {
            requireStorPoolDfnChangeAccess();

            StorPoolDefinitionData storPoolDfn = loadStorPoolDfn(storPoolNameStr, true);

            Iterator<StorPool> storPoolIterator = getPrivilegedStorPoolIterator(storPoolDfn);

            if (storPoolIterator.hasNext())
            {
                StringBuilder nodeNames = new StringBuilder();
                nodeNames.append("'");
                while (storPoolIterator.hasNext())
                {
                    nodeNames.append(storPoolIterator.next().getNode().getName().displayValue)
                             .append("', '");
                }
                nodeNames.setLength(nodeNames.length() - ", '".length());

                addAnswer(
                    getObjectDescription() + " has still storage pools on node(s): " + nodeNames + ".",
                    null, // cause
                    null, // details
                    "Remove the storage pools first.", // correction
                    ApiConsts.FAIL_IN_USE
                );
            }
            else
            {
                UUID storPoolDfnUuid = storPoolDfn.getUuid();
                StorPoolName storPoolName = storPoolDfn.getName();
                delete(storPoolDfn);
                commit();

                storPoolDfnMap.remove(storPoolName);

                reportSuccess(storPoolDfnUuid);
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
                getObjectDescriptionInline(storPoolNameStr),
                getObjRefs(storPoolNameStr),
                getVariables(storPoolNameStr),
                apiCallRc,
                accCtx,
                client
            );
        }

        return apiCallRc;
    }

    private void delete(StorPoolDefinitionData storPoolDfn)
    {
        try
        {
            storPoolDfn.delete(currentAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "delete " + getObjectDescriptionInline(),
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL_DFN
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

    private Iterator<StorPool> getPrivilegedStorPoolIterator(StorPoolDefinitionData storPoolDfn)
    {
        Iterator<StorPool> iterator;
        try
        {
            iterator = storPoolDfn.iterateStorPools(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
        return iterator;
    }

    byte[] listStorPoolDefinitions(int msgId, AccessContext accCtx)
    {
        ArrayList<StorPoolDefinitionData.StorPoolDfnApi> storPoolDfns = new ArrayList<>();
        try
        {
            storPoolDfnMapProt.requireAccess(accCtx, AccessType.VIEW);
            for (StorPoolDefinition storPoolDfn : storPoolDfnMap.values())
            {
                try
                {
                    if (!storPoolDfn.getName().getDisplayName().equals(LinStor.DISKLESS_STOR_POOL_NAME))
                    {
                        storPoolDfns.add(storPoolDfn.getApiData(accCtx));
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
            .builder(ApiConsts.API_LST_STOR_POOL_DFN, msgId)
            .storPoolDfnList(storPoolDfns)
            .build();
    }

    protected AbsApiCallHandler setContext(
        AccessContext accCtx,
        Peer peer,
        ApiCallType type,
        ApiCallRcImpl apiCallRc,
        TransactionMgr transMgr,
        String storPoolNameStr
    )
    {
        super.setContext(
            accCtx,
            peer,
            type,
            apiCallRc,
            transMgr,
            getObjRefs(storPoolNameStr),
            getVariables(storPoolNameStr)
        );

        currentStorPoolNameStr.set(storPoolNameStr);

        return this;
    }

    private StorPoolDefinitionData createStorPool(String storPoolNameStr)
    {
        StorPoolDefinitionData storPoolDfn;
        try
        {
            storPoolDfn = StorPoolDefinitionData.getInstance(
                currentAccCtx.get(),
                asStorPoolName(storPoolNameStr),
                currentTransMgr.get(),
                true, // persist this entry
                true // fail if already exists
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "create " + getObjectDescriptionInline(),
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL_DFN
            );
        }
        catch (LinStorDataAlreadyExistsException alreadyExistsExc)
        {
            throw asExc(
                alreadyExistsExc,
                getObjectDescription() + " already exists.",
                ApiConsts.FAIL_EXISTS_STOR_POOL_DFN
            );
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(
                sqlExc,
                "creating " + getObjectDescriptionInline()
            );
        }
        return storPoolDfn;
    }

    private Map<String, String> getObjRefs(String storPoolNameStr)
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_STOR_POOL_DFN, storPoolNameStr);
        return objRefs;
    }

    private Map<String, String> getVariables(String storPoolNameStr)
    {
        Map<String, String> vars = new TreeMap<>();
        vars.put(ApiConsts.KEY_STOR_POOL_NAME, storPoolNameStr);
        return vars;
    }

    private void requireStorPoolDfnChangeAccess()
    {
        try
        {
            storPoolDfnMapProt.requireAccess(
                currentAccCtx.get(),
                AccessType.CHANGE
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "change any storage definitions pools.",
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL_DFN
            );
        }
    }
    @Override
    protected String getObjectDescription()
    {
        return "Storage pool definition: " + currentStorPoolNameStr.get();
    }

    @Override
    protected String getObjectDescriptionInline()
    {
        return getObjectDescriptionInline(currentStorPoolNameStr.get());
    }


    private String getObjectDescriptionInline(String storPoolName)
    {
        return "storage pool definition '" + storPoolName + "'";
    }

    private Props getProps(StorPoolDefinitionData storPoolDfn)
    {
        Props props;
        try
        {
            props = storPoolDfn.getProps(currentAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "accessing properties of storage pool definition '" + storPoolDfn.getName().displayValue + "'.",
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL_DFN
            );
        }
        return props;
    }

    private void updateSatellites(StorPoolDefinitionData storPoolDfn)
    {
        try
        {
            Iterator<StorPool> iterateStorPools = storPoolDfn.iterateStorPools(apiCtx);
            while (iterateStorPools.hasNext())
            {
                StorPool storPool = iterateStorPools.next();
                updateSatellite(storPool);
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
    }
}
