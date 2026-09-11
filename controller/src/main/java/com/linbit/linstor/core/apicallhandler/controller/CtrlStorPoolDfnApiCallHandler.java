package com.linbit.linstor.core.apicallhandler.controller;


import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.apis.StorPoolDefinitionApi;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.objects.StorPoolDefinitionControllerFactory;
import com.linbit.linstor.core.repository.StorPoolDefinitionRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.locks.LockGuardFactory;

import static com.linbit.locks.LockGuardFactory.LockObj.NODES_MAP;
import static com.linbit.locks.LockGuardFactory.LockObj.STOR_POOL_DFN_MAP;
import static com.linbit.locks.LockGuardFactory.LockType.WRITE;
import static com.linbit.utils.StringUtils.firstLetterCaps;

import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import reactor.core.publisher.Flux;

@Singleton
class CtrlStorPoolDfnApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final StorPoolDefinitionControllerFactory storPoolDefinitionFactory;
    private final StorPoolDefinitionRepository storPoolDefinitionRepository;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final ResponseConverter responseConverter;
    private final Provider<Peer> peer;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;

    @Inject
    CtrlStorPoolDfnApiCallHandler(
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        StorPoolDefinitionControllerFactory storPoolDefinitionFactoryRef,
        StorPoolDefinitionRepository storPoolDefinitionRepositoryRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ResponseConverter responseConverterRef,
        Provider<Peer> peerRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        ErrorReporter errorReporterRef
    )
    {
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        storPoolDefinitionFactory = storPoolDefinitionFactoryRef;
        storPoolDefinitionRepository = storPoolDefinitionRepositoryRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        responseConverter = responseConverterRef;
        peer = peerRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        errorReporter = errorReporterRef;
    }

    public ApiCallRc createStorPoolDfn(
        String storPoolNameStr,
        Map<String, String> storPoolDfnProps
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeStorPoolDfnContext(
            ApiOperation.makeCreateOperation(),
            storPoolNameStr
        );

        try
        {
            requireStorPoolDfnChangeAccess();

            StorPoolDefinition storPoolDfn = createStorPool(storPoolNameStr);
            ctrlPropsHelper.fillProperties(
                responses, LinStorObject.STOR_POOL_DFN, storPoolDfnProps,
                getProps(storPoolDfn), ApiConsts.FAIL_ACC_DENIED_STOR_POOL_DFN);

            storPoolDefinitionRepository.put(storPoolDfn.getName(), storPoolDfn);
            ctrlTransactionHelper.commit();

            responseConverter.addWithOp(responses, context, ApiSuccessUtils.defaultCreatedEntry(
                storPoolDfn.getUuid(), getStorPoolDfnDescriptionInline(storPoolDfn)));
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    public Flux<ApiCallRc> modify(
        @Nullable UUID storPoolDfnUuid,
        String storPoolNameStr,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        Set<String> deletePropNamespaces
    )
    {
        ResponseContext context = makeStorPoolDfnContext(
            ApiOperation.makeModifyOperation(),
            storPoolNameStr
        );

        return scopeRunner
            .fluxInTransactionalScope(
                "Modify storage-pool-definition",
                lockGuardFactory.buildDeferred(WRITE, NODES_MAP, STOR_POOL_DFN_MAP),
                () -> modifyInTransaction(
                    storPoolDfnUuid,
                    storPoolNameStr,
                    overrideProps,
                    deletePropKeys,
                    deletePropNamespaces,
                    context
                )
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> modifyInTransaction(
        @Nullable UUID storPoolDfnUuid,
        String storPoolNameStr,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        Set<String> deletePropNamespaces,
        ResponseContext context
    )
    {
        List<Flux<Flux<ApiCallRc>>> fluxes = new ArrayList<>();
        ApiCallRcImpl apiCallRcs = new ApiCallRcImpl();
        boolean notifyStlts;

        try
        {
            requireStorPoolDfnChangeAccess();
            StorPoolDefinition storPoolDfn = ctrlApiDataLoader.loadStorPoolDfn(storPoolNameStr);

            if (storPoolDfnUuid != null && !storPoolDfnUuid.equals(storPoolDfn.getUuid()))
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UUID_STOR_POOL_DFN,
                    "UUID-check failed"
                ));
            }

            Props props = getProps(storPoolDfn);

            notifyStlts = ctrlPropsHelper.fillProperties(
                apiCallRcs, LinStorObject.STOR_POOL_DFN, overrideProps,
                getProps(storPoolDfn), ApiConsts.FAIL_ACC_DENIED_STOR_POOL_DFN);
            notifyStlts = ctrlPropsHelper.remove(
                apiCallRcs,
                LinStorObject.STOR_POOL_DFN,
                props,
                deletePropKeys,
                deletePropNamespaces) || notifyStlts;

            ctrlTransactionHelper.commit();

            responseConverter.addWithOp(apiCallRcs, context, ApiSuccessUtils.defaultModifiedEntry(
                storPoolDfn.getUuid(), getStorPoolDfnDescriptionInline(storPoolDfn)));

            if (notifyStlts)
            {
                fluxes = updateSatellites(storPoolDfn);
            }
        }
        catch (Exception | ImplementationError exc)
        {
            apiCallRcs = responseConverter.reportException(peer.get(), context, exc);
        }

        return Flux.just((ApiCallRc) apiCallRcs)
            .concatWith(CtrlResponseUtils.mergeExtractingApiRcExceptions(errorReporter, Flux.merge(fluxes)));
    }

    public ApiCallRc deleteStorPoolDfn(String storPoolNameStr)
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeStorPoolDfnContext(
            ApiOperation.makeDeleteOperation(),
            storPoolNameStr
        );

        try
        {
            requireStorPoolDfnChangeAccess();

            StorPoolDefinition storPoolDfn = ctrlApiDataLoader.loadStorPoolDfn(storPoolNameStr);

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

                storPoolDefinitionRepository.remove(storPoolName);
                ctrlTransactionHelper.commit();

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

    private void delete(StorPoolDefinition storPoolDfn)
    {
        try
        {
            storPoolDfn.delete();
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private Iterator<StorPool> getPrivilegedStorPoolIterator(StorPoolDefinition storPoolDfn)
    {
        Iterator<StorPool> iterator;
        iterator = storPoolDfn.iterateStorPools();
        return iterator;
    }

    ArrayList<StorPoolDefinitionApi> listStorPoolDefinitions()
    {
        ArrayList<StorPoolDefinitionApi> storPoolDfns = new ArrayList<>();
        for (StorPoolDefinition storPoolDfn : storPoolDefinitionRepository.getMapForView().values())
        {
            if (!storPoolDfn.getName().getDisplayName().equals(LinStor.DISKLESS_STOR_POOL_NAME))
            {
                storPoolDfns.add(storPoolDfn.getApiData());
            }
        }

        return storPoolDfns;
    }

    private StorPoolDefinition createStorPool(String storPoolNameStrRef)
    {
        StorPoolDefinition storPoolDfn;
        try
        {
            storPoolDfn = storPoolDefinitionFactory.create(
                LinstorParsingUtils.asStorPoolName(storPoolNameStrRef)
            );
        }
        catch (LinStorDataAlreadyExistsException alreadyExistsExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_EXISTS_STOR_POOL_DFN,
                firstLetterCaps(getStorPoolDfnDescriptionInline(storPoolNameStrRef)) + " already exists.",
                true
            ), alreadyExistsExc);
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
        return storPoolDfn;
    }

    private void requireStorPoolDfnChangeAccess()
    {
    }

    private Props getProps(StorPoolDefinition storPoolDfn)
    {
        Props props;
        props = storPoolDfn.getProps();
        return props;
    }

    private List<Flux<Flux<ApiCallRc>>> updateSatellites(StorPoolDefinition storPoolDfn)
    {
        List<Flux<Flux<ApiCallRc>>> fluxes = new ArrayList<>();

        Iterator<StorPool> iterateStorPools = storPoolDfn.iterateStorPools();
        while (iterateStorPools.hasNext())
        {
            fluxes.add(
                Flux.just(
                ctrlSatelliteUpdateCaller.updateSatellite(iterateStorPools.next())
                    .transform(
                        updateResponses -> CtrlResponseUtils.combineResponses(
                            errorReporter,
                            updateResponses,
                            storPoolDfn.getName().displayValue,
                            Collections.emptyList(),
                            "Storage pool updated on {0}",
                            "Storage pool updated on {0}"
                        )
                    )
                )
            );
        }

        return fluxes;
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
        ApiOperation operation,
        String storPoolNameStr
    )
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_STOR_POOL_DFN, storPoolNameStr);

        return new ResponseContext(
            operation,
            getStorPoolDfnDescription(storPoolNameStr),
            getStorPoolDfnDescriptionInline(storPoolNameStr),
            ApiConsts.MASK_STOR_POOL_DFN,
            objRefs
        );
    }
}
