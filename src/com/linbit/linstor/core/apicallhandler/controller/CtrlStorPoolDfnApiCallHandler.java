package com.linbit.linstor.core.apicallhandler.controller;


import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.StorPoolDefinitionData;
import com.linbit.linstor.StorPoolDefinitionDataControllerFactory;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CtrlObjectFactories;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.AbsApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlAutoStorPoolSelector.AutoStorPoolSelectorConfig;
import com.linbit.linstor.core.apicallhandler.controller.CtrlAutoStorPoolSelector.Candidate;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSQLException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
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
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import static com.linbit.utils.StringUtils.firstLetterCaps;

@Singleton
class CtrlStorPoolDfnApiCallHandler extends AbsApiCallHandler
{
    private final CtrlClientSerializer clientComSerializer;
    private final CoreModule.StorPoolDefinitionMap storPoolDfnMap;
    private final ObjectProtection storPoolDfnMapProt;
    private final StorPoolDefinitionDataControllerFactory storPoolDefinitionDataFactory;
    private final CtrlAutoStorPoolSelector autoStorPoolSelector;
    private final Provider<Integer> msgIdProvider;
    private final ResponseConverter responseConverter;

    @Inject
    CtrlStorPoolDfnApiCallHandler(
        ErrorReporter errorReporterRef,
        CtrlStltSerializer interComSerializer,
        CtrlClientSerializer clientComSerializerRef,
        @ApiContext AccessContext apiCtxRef,
        CoreModule.StorPoolDefinitionMap storPoolDfnMapRef,
        @Named(ControllerSecurityModule.STOR_POOL_DFN_MAP_PROT) ObjectProtection storPoolDfnMapProtRef,
        CtrlObjectFactories objectFactories,
        StorPoolDefinitionDataControllerFactory storPoolDefinitionDataFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        Provider<Peer> peerRef,
        WhitelistProps whiteListProps,
        CtrlAutoStorPoolSelector autoStorPoolSelectorRef,
        @Named(ApiModule.MSG_ID) Provider<Integer> msgIdProviderRef,
        ResponseConverter responseConverterRef
    )
    {
        super(
            errorReporterRef,
            apiCtxRef,
            interComSerializer,
            objectFactories,
            transMgrProviderRef,
            peerAccCtxRef,
            peerRef,
            whiteListProps
        );
        clientComSerializer = clientComSerializerRef;
        storPoolDfnMap = storPoolDfnMapRef;
        storPoolDfnMapProt = storPoolDfnMapProtRef;
        storPoolDefinitionDataFactory = storPoolDefinitionDataFactoryRef;
        autoStorPoolSelector = autoStorPoolSelectorRef;
        msgIdProvider = msgIdProviderRef;
        responseConverter = responseConverterRef;
    }

    public ApiCallRc createStorPoolDfn(
        String storPoolNameStr,
        Map<String, String> storPoolDfnProps
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeStorPoolDfnContext(
            peer.get(),
            ApiOperation.makeCreateOperation(),
            storPoolNameStr
        );

        try
        {
            requireStorPoolDfnChangeAccess();

            StorPoolDefinitionData storPoolDfn = createStorPool(storPoolNameStr);
            fillProperties(LinStorObject.STORAGEPOOL_DEFINITION, storPoolDfnProps, getProps(storPoolDfn),
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL_DFN);
            commit();

            storPoolDfnMap.put(storPoolDfn.getName(), storPoolDfn);
            responseConverter.addWithOp(responses, context, ApiSuccessUtils.defaultCreatedEntry(
                storPoolDfn.getUuid(), getStorPoolDfnDescriptionInline(storPoolDfn)));
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    public ApiCallRc modifyStorPoolDfn(
        UUID storPoolDfnUuid,
        String storPoolNameStr,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeStorPoolDfnContext(
            peer.get(),
            ApiOperation.makeModifyOperation(),
            storPoolNameStr
        );

        try
        {
            requireStorPoolDfnChangeAccess();
            StorPoolDefinitionData storPoolDfn = loadStorPoolDfn(storPoolNameStr, true);

            if (storPoolDfnUuid != null && !storPoolDfnUuid.equals(storPoolDfn.getUuid()))
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UUID_STOR_POOL_DFN,
                    "UUID-check failed"
                ));
            }

            Props props = getProps(storPoolDfn);
            Map<String, String> propsMap = props.map();

            fillProperties(LinStorObject.STORAGEPOOL_DEFINITION, overrideProps, getProps(storPoolDfn),
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL_DFN);

            for (String delKey : deletePropKeys)
            {
                propsMap.remove(delKey);
            }

            commit();

            responseConverter.addWithDetail(responses, context, updateSatellites(storPoolDfn));
            responseConverter.addWithOp(responses, context, ApiSuccessUtils.defaultModifiedEntry(
                storPoolDfn.getUuid(), getStorPoolDfnDescriptionInline(storPoolDfn)));
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    public ApiCallRc deleteStorPoolDfn(String storPoolNameStr)
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeStorPoolDfnContext(
            peer.get(),
            ApiOperation.makeDeleteOperation(),
            storPoolNameStr
        );

        try
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

                responseConverter.addWithDetail(responses, context, ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_IN_USE,
                        firstLetterCaps(getStorPoolDfnDescriptionInline(storPoolNameStr)) +
                            " has still storage pools on node(s): " + nodeNames + "."
                    )
                    .setCorrection("Remove the storage pools first.")
                    .build()
                );
            }
            else
            {
                UUID storPoolDfnUuid = storPoolDfn.getUuid();
                StorPoolName storPoolName = storPoolDfn.getName();
                delete(storPoolDfn);
                commit();

                storPoolDfnMap.remove(storPoolName);

                responseConverter.addWithOp(responses, context, ApiSuccessUtils.defaultDeletedEntry(
                    storPoolDfnUuid, getStorPoolDfnDescriptionInline(storPoolName.displayValue)));
            }
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    public byte[] getMaxVlmSizeForReplicaCount(AutoSelectFilterApi selectFilter)
    {
        byte[] result;
        List<Candidate> candidateList = null;
        StorPoolName storPoolName = null;
        String storPoolNameStr = selectFilter.getStorPoolNameStr();
        if (storPoolNameStr != null && storPoolNameStr.length() > 0)
        {
            storPoolName = asStorPoolName(storPoolNameStr);
        }
        candidateList = autoStorPoolSelector.getCandidateList(
            0L,
            new AutoStorPoolSelectorConfig(selectFilter),
            CtrlAutoStorPoolSelector::mostRemainingSpaceNodeStrategy
        );
        if (candidateList.isEmpty())
        {
            ApiCallRcImpl errRc = new ApiCallRcImpl();
            if (storPoolName != null && storPoolDfnMap.get(storPoolName) == null)
            {
                // check if storpooldfn exists (storPoolName is known)
                errRc.addEntry(
                    "Unknown storage pool name",
                    ApiConsts.MASK_ERROR | ApiConsts.FAIL_INVLD_STOR_POOL_NAME
                );
            }
            else
            {
                // else we simply have not enough nodes
                errRc.addEntry(
                    "Not enough nodes",
                    ApiConsts.MASK_ERROR | ApiConsts.FAIL_NOT_ENOUGH_NODES
                );

            }
            result = clientComSerializer
                .builder(ApiConsts.API_REPLY, msgIdProvider.get())
                .apiCallRcSeries(errRc)
                .build();
        }
        else
        {
            candidateList.sort(Comparator.comparingLong(c -> c.sizeAfterDeployment));

            result = clientComSerializer
                .builder(ApiConsts.API_RSP_MAX_VLM_SIZE, msgIdProvider.get())
                .maxVlmSizeCandidateList(candidateList)
                .build();
        }
        return result;
    }

    private void delete(StorPoolDefinitionData storPoolDfn)
    {
        try
        {
            storPoolDfn.delete(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "delete " + getStorPoolDfnDescriptionInline(storPoolDfn),
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL_DFN
            );
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
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
            throw new ImplementationError(accDeniedExc);
        }
        return iterator;
    }

    byte[] listStorPoolDefinitions(int msgId)
    {
        ArrayList<StorPoolDefinitionData.StorPoolDfnApi> storPoolDfns = new ArrayList<>();
        try
        {
            storPoolDfnMapProt.requireAccess(peerAccCtx.get(), AccessType.VIEW);
            for (StorPoolDefinition storPoolDfn : storPoolDfnMap.values())
            {
                try
                {
                    if (!storPoolDfn.getName().getDisplayName().equals(LinStor.DISKLESS_STOR_POOL_NAME))
                    {
                        storPoolDfns.add(storPoolDfn.getApiData(peerAccCtx.get()));
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

    private StorPoolDefinitionData createStorPool(String storPoolNameStrRef)
    {
        StorPoolDefinitionData storPoolDfn;
        try
        {
            storPoolDfn = storPoolDefinitionDataFactory.getInstance(
                peerAccCtx.get(),
                asStorPoolName(storPoolNameStrRef),
                true, // persist this entry
                true // fail if already exists
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "create " + getStorPoolDfnDescriptionInline(storPoolNameStrRef),
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL_DFN
            );
        }
        catch (LinStorDataAlreadyExistsException alreadyExistsExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_EXISTS_STOR_POOL_DFN,
                firstLetterCaps(getStorPoolDfnDescriptionInline(storPoolNameStrRef)) + " already exists."
            ), alreadyExistsExc);
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
        return storPoolDfn;
    }

    private void requireStorPoolDfnChangeAccess()
    {
        try
        {
            storPoolDfnMapProt.requireAccess(
                peerAccCtx.get(),
                AccessType.CHANGE
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "change any storage definitions pools",
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL_DFN
            );
        }
    }

    private Props getProps(StorPoolDefinitionData storPoolDfn)
    {
        Props props;
        try
        {
            props = storPoolDfn.getProps(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access properties of storage pool definition '" + storPoolDfn.getName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL_DFN
            );
        }
        return props;
    }

    private ApiCallRc updateSatellites(StorPoolDefinitionData storPoolDfn)
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        try
        {
            Iterator<StorPool> iterateStorPools = storPoolDfn.iterateStorPools(apiCtx);
            while (iterateStorPools.hasNext())
            {
                StorPool storPool = iterateStorPools.next();
                responses.addEntries(updateSatellite(storPool));
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }

        return responses;
    }

    public static String getStorPoolDfnDescription(String storPoolName)
    {
        return "Storage pool definition: " + storPoolName;
    }

    public static String getStorPoolDfnDescriptionInline(StorPoolDefinition storPoolDfn)
    {
        return getStorPoolDfnDescriptionInline(storPoolDfn.getName().displayValue);
    }

    public static String getStorPoolDfnDescriptionInline(String storPoolName)
    {
        return "storage pool definition '" + storPoolName + "'";
    }

    private static ResponseContext makeStorPoolDfnContext(
        Peer peer,
        ApiOperation operation,
        String storPoolNameStr
    )
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_STOR_POOL_DFN, storPoolNameStr);

        return new ResponseContext(
            peer,
            operation,
            getStorPoolDfnDescription(storPoolNameStr),
            getStorPoolDfnDescriptionInline(storPoolNameStr),
            ApiConsts.MASK_STOR_POOL_DFN,
            objRefs
        );
    }
}
