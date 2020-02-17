package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.helpers.StorPoolHelper;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.locks.LockGuardFactory;

import static com.linbit.linstor.core.apicallhandler.controller.helpers.StorPoolHelper.getStorPoolDescription;
import static com.linbit.linstor.core.apicallhandler.controller.helpers.StorPoolHelper.getStorPoolDescriptionInline;
import static com.linbit.locks.LockGuardFactory.LockObj.NODES_MAP;
import static com.linbit.locks.LockGuardFactory.LockObj.STOR_POOL_DFN_MAP;
import static com.linbit.locks.LockGuardFactory.LockType.WRITE;
import static com.linbit.utils.StringUtils.firstLetterCaps;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlStorPoolApiCallHandler
{
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final ResponseConverter responseConverter;
    private final Provider<Peer> peer;
    private final Provider<AccessContext> peerAccCtx;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;

    @Inject
    public CtrlStorPoolApiCallHandler(
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ResponseConverter responseConverterRef,
        Provider<Peer> peerRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef
    )
    {
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        responseConverter = responseConverterRef;
        peer = peerRef;
        peerAccCtx = peerAccCtxRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
    }

    public Flux<ApiCallRc> modify(
        UUID storPoolUuid,
        String nodeNameStr,
        String storPoolNameStr,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        Set<String> deletePropNamespaces
    )
    {
        ResponseContext context = makeStorPoolContext(
            ApiOperation.makeModifyOperation(),
            nodeNameStr,
            storPoolNameStr
        );

        return scopeRunner
            .fluxInTransactionalScope(
                "Modify storage-pool",
                lockGuardFactory.buildDeferred(WRITE, NODES_MAP, STOR_POOL_DFN_MAP),
                () -> modifyInTransaction(
                    storPoolUuid,
                    nodeNameStr,
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
        UUID storPoolUuid,
        String nodeNameStr,
        String storPoolNameStr,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        Set<String> deletePropNamespaces,
        ResponseContext context
    )
    {
        Flux<ApiCallRc> flux = Flux.empty();
        ApiCallRcImpl apiCallRcs = new ApiCallRcImpl();

        try
        {
            StorPool storPool = loadStorPool(nodeNameStr, storPoolNameStr, true);

            if (storPoolUuid != null && !storPoolUuid.equals(storPool.getUuid()))
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UUID_STOR_POOL,
                    "UUID-check failed"
                ));
            }

            Props props = ctrlPropsHelper.getProps(storPool);

            // check if specified preferred network interface exists
            ctrlPropsHelper.checkPrefNic(
                peerAccCtx.get(),
                storPool.getNode(),
                overrideProps.get(ApiConsts.KEY_STOR_POOL_PREF_NIC),
                ApiConsts.MASK_STOR_POOL
            );
            ctrlPropsHelper.checkPrefNic(
                peerAccCtx.get(),
                storPool.getNode(),
                overrideProps.get(ApiConsts.NAMESPC_NVME + "/" + ApiConsts.KEY_PREF_NIC),
                ApiConsts.MASK_STOR_POOL
            );

            ctrlPropsHelper.fillProperties(
                apiCallRcs,
                LinStorObject.STORAGEPOOL,
                overrideProps,
                props,
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL
            );
            ctrlPropsHelper.remove(
                apiCallRcs, LinStorObject.STORAGEPOOL, props, deletePropKeys, deletePropNamespaces);

            // check if specified preferred network interface exists
            ctrlPropsHelper.checkPrefNic(
                    peerAccCtx.get(),
                    storPool.getNode(),
                    overrideProps.get(ApiConsts.KEY_STOR_POOL_PREF_NIC),
                    ApiConsts.MASK_STOR_POOL
            );

            ctrlTransactionHelper.commit();

            responseConverter.addWithOp(apiCallRcs, context, ApiSuccessUtils.defaultModifiedEntry(
                storPool.getUuid(), getStorPoolDescriptionInline(storPool)));

            flux = ctrlSatelliteUpdateCaller.updateSatellite(storPool);
        }
        catch (Exception | ImplementationError exc)
        {
            apiCallRcs = responseConverter.reportException(peer.get(), context, exc);
        }

        return Flux.<ApiCallRc>just(apiCallRcs).concatWith(flux);
    }

    public Flux<ApiCallRc> deleteStorPool(
        String nodeNameStr,
        String storPoolNameStr
    )
    {
        ResponseContext context = makeStorPoolContext(
            ApiOperation.makeModifyOperation(),
            nodeNameStr,
            storPoolNameStr
        );

        return scopeRunner
            .fluxInTransactionalScope(
                "Delete storage-pool",
                lockGuardFactory.buildDeferred(WRITE, NODES_MAP, STOR_POOL_DFN_MAP),
                () -> deleteStorPoolInTransaction(
                    nodeNameStr,
                    storPoolNameStr
                )
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    public Flux<ApiCallRc> deleteStorPoolInTransaction(
        String nodeNameStr,
        String storPoolNameStr
    )
    {
        Flux<ApiCallRc> flux = Flux.empty();
        ApiCallRcImpl apiCallRcs = new ApiCallRcImpl();
        ResponseContext context = makeStorPoolContext(
            ApiOperation.makeDeleteOperation(),
            nodeNameStr,
            storPoolNameStr
        );

        try
        {
            if (LinStor.DISKLESS_STOR_POOL_NAME.equalsIgnoreCase(storPoolNameStr))
            {
                throw new ApiRcException(
                    ApiCallRcImpl.entryBuilder(
                        ApiConsts.FAIL_INVLD_STOR_POOL_NAME,
                        "The default diskless storage pool cannot be deleted!"
                    )
                    .build()
                );
            }

            StorPool storPool = loadStorPool(nodeNameStr, storPoolNameStr, false);

            if (storPool == null)
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.WARN_NOT_FOUND,
                        "Deletion of " + getStorPoolDescriptionInline(nodeNameStr, storPoolNameStr) + " had no effect."
                    )
                    .setCause(firstLetterCaps(getStorPoolDescriptionInline(nodeNameStr, storPoolNameStr)) +
                        " does not exist.")
                    .build()
                );
            }
            Collection<VlmProviderObject<Resource>> volumes = getVolumes(storPool);
            if (!volumes.isEmpty())
            {
                StringBuilder volListSb = new StringBuilder();
                for (VlmProviderObject<Resource> vlmObj : volumes)
                {
                    Resource rsc = vlmObj.getVolume().getAbsResource();
                    volListSb.append("\n   Node name: '")
                         .append(rsc.getNode().getName().displayValue)
                         .append("', resource name: '")
                         .append(rsc.getDefinition().getName().displayValue)
                         .append("', volume number: ")
                         .append(vlmObj.getVlmNr().value);
                }

                String correction = volumes.size() == 1 ?
                    "Delete the listed volume first." :
                    "Delete the listed volumes first.";
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_IN_USE,
                        String.format(
                                "The specified storage pool '%s' on node '%s' can not be deleted as " +
                                    "volumes are still using it.",
                                storPoolNameStr,
                                nodeNameStr
                            )
                    )
                    .setDetails("Volumes that are still using the storage pool: " + volListSb.toString())
                    .setCorrection(correction)
                    .build()
                );
            }
            else
            {
                UUID storPoolUuid = storPool.getUuid(); // cache storpool uuid to avoid access deleted storpool
                final Node storPoolNode = storPool.getNode();
                delete(storPool);
                ctrlTransactionHelper.commit();

                responseConverter.addWithOp(apiCallRcs, context, ApiSuccessUtils.defaultDeletedEntry(
                    storPoolUuid, getStorPoolDescription(nodeNameStr, storPoolNameStr)));

                flux = ctrlSatelliteUpdateCaller.updateSatellite(storPoolUuid, storPoolNameStr, storPoolNode);
            }
        }
        catch (Exception | ImplementationError exc)
        {
            apiCallRcs = responseConverter.reportException(peer.get(), context, exc);
        }

        return Flux.<ApiCallRc>just(apiCallRcs).concatWith(flux);
    }

    private Collection<VlmProviderObject<Resource>> getVolumes(StorPool storPool)
    {
        Collection<VlmProviderObject<Resource>> volumes;
        try
        {
            volumes = storPool.getVolumes(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access the volumes of " + getStorPoolDescriptionInline(storPool),
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL
            );
        }
        return volumes;
    }

    private void delete(StorPool storPool)
    {
        try
        {
            storPool.delete(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "delete " + StorPoolHelper.getStorPoolDescriptionInline(storPool),
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private StorPool loadStorPool(String nodeNameStr, String storPoolNameStr, boolean failIfNull)
    {
        return ctrlApiDataLoader.loadStorPool(
            ctrlApiDataLoader.loadStorPoolDfn(storPoolNameStr, true),
            ctrlApiDataLoader.loadNode(nodeNameStr, true),
            failIfNull
        );
    }

    public static ResponseContext makeStorPoolContext(
        ApiOperation operation,
        String nodeNameStr,
        String storPoolNameStr
    )
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_NODE, nodeNameStr);
        objRefs.put(ApiConsts.KEY_STOR_POOL_DFN, storPoolNameStr);

        return new ResponseContext(
            operation,
            getStorPoolDescription(nodeNameStr, storPoolNameStr),
            getStorPoolDescriptionInline(nodeNameStr, storPoolNameStr),
            ApiConsts.MASK_STOR_POOL,
            objRefs
        );
    }
}
