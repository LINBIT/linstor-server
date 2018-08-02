package com.linbit.linstor.core.apicallhandler.controller;


import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.StorPoolDefinitionData;
import com.linbit.linstor.StorPoolDefinitionDataControllerFactory;
import com.linbit.linstor.StorPoolDefinitionRepository;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.controller.CtrlAutoStorPoolSelector.AutoStorPoolSelectorConfig;
import com.linbit.linstor.core.apicallhandler.controller.CtrlAutoStorPoolSelector.Candidate;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSQLException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ControllerSecurityModule;
import com.linbit.linstor.security.ObjectProtection;

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
class CtrlStorPoolDfnApiCallHandler
{
    private final AccessContext apiCtx;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlAutoStorPoolSelector autoStorPoolSelector;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final StorPoolDefinitionDataControllerFactory storPoolDefinitionDataFactory;
    private final StorPoolDefinitionRepository storPoolDefinitionRepository;
    private final CtrlClientSerializer clientComSerializer;
    private final CtrlSatelliteUpdater ctrlSatelliteUpdater;
    private final ResponseConverter responseConverter;
    private final Provider<Peer> peer;
    private final Provider<AccessContext> peerAccCtx;
    private final Provider<Integer> msgIdProvider;

    @Inject
    CtrlStorPoolDfnApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlAutoStorPoolSelector autoStorPoolSelectorRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        StorPoolDefinitionDataControllerFactory storPoolDefinitionDataFactoryRef,
        StorPoolDefinitionRepository storPoolDefinitionRepositoryRef,
        CtrlClientSerializer clientComSerializerRef,
        CtrlSatelliteUpdater ctrlSatelliteUpdaterRef,
        ResponseConverter responseConverterRef,
        Provider<Peer> peerRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        @Named(ApiModule.MSG_ID) Provider<Integer> msgIdProviderRef
    )
    {
        apiCtx = apiCtxRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        autoStorPoolSelector = autoStorPoolSelectorRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        storPoolDefinitionDataFactory = storPoolDefinitionDataFactoryRef;
        storPoolDefinitionRepository = storPoolDefinitionRepositoryRef;
        clientComSerializer = clientComSerializerRef;
        ctrlSatelliteUpdater = ctrlSatelliteUpdaterRef;
        responseConverter = responseConverterRef;
        peer = peerRef;
        peerAccCtx = peerAccCtxRef;
        msgIdProvider = msgIdProviderRef;
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
            ctrlPropsHelper.fillProperties(LinStorObject.STORAGEPOOL_DEFINITION, storPoolDfnProps,
                getProps(storPoolDfn), ApiConsts.FAIL_ACC_DENIED_STOR_POOL_DFN);
            ctrlTransactionHelper.commit();

            storPoolDefinitionRepository.put(apiCtx, storPoolDfn.getName(), storPoolDfn);
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
            StorPoolDefinitionData storPoolDfn = ctrlApiDataLoader.loadStorPoolDfn(storPoolNameStr, true);

            if (storPoolDfnUuid != null && !storPoolDfnUuid.equals(storPoolDfn.getUuid()))
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UUID_STOR_POOL_DFN,
                    "UUID-check failed"
                ));
            }

            Props props = getProps(storPoolDfn);
            Map<String, String> propsMap = props.map();

            ctrlPropsHelper.fillProperties(LinStorObject.STORAGEPOOL_DEFINITION, overrideProps,
                getProps(storPoolDfn), ApiConsts.FAIL_ACC_DENIED_STOR_POOL_DFN);

            for (String delKey : deletePropKeys)
            {
                propsMap.remove(delKey);
            }

            ctrlTransactionHelper.commit();

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

            StorPoolDefinitionData storPoolDfn = ctrlApiDataLoader.loadStorPoolDfn(storPoolNameStr, true);

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
                ctrlTransactionHelper.commit();

                storPoolDefinitionRepository.remove(apiCtx, storPoolName);

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
            storPoolName = LinstorParsingUtils.asStorPoolName(storPoolNameStr);
        }
        candidateList = autoStorPoolSelector.getCandidateList(
            0L,
            new AutoStorPoolSelectorConfig(selectFilter),
            CtrlAutoStorPoolSelector::mostRemainingSpaceNodeStrategy
        );
        if (candidateList.isEmpty())
        {
            ApiCallRcImpl errRc = new ApiCallRcImpl();
            try
            {
                if (storPoolName != null && storPoolDefinitionRepository.get(peerAccCtx.get(), storPoolName) == null)
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
            }
            catch (AccessDeniedException exc)
            {
                errRc.addEntry(
                    "Access to storage pool definition denied",
                    ApiConsts.MASK_ERROR | ApiConsts.FAIL_ACC_DENIED_STOR_POOL_DFN
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
            for (StorPoolDefinition storPoolDfn : storPoolDefinitionRepository.getMapForView(peerAccCtx.get()).values())
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
            storPoolDfn = storPoolDefinitionDataFactory.create(
                peerAccCtx.get(),
                LinstorParsingUtils.asStorPoolName(storPoolNameStrRef)
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
            storPoolDefinitionRepository.requireAccess(
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
                responses.addEntries(ctrlSatelliteUpdater.updateSatellite(storPool));
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
