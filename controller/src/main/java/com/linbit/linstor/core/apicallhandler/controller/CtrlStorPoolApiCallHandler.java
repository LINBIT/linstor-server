package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.CtrlSecurityObjects;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.helpers.EncryptionHelper;
import com.linbit.linstor.core.apicallhandler.controller.helpers.StorPoolHelper;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.repository.StorPoolDefinitionRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.locks.LockGuardFactory;
import com.linbit.utils.Base64;

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
import java.util.Collections;
import java.util.Iterator;
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
    private final AccessContext sysCtx;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlSecurityObjects securityObjects;
    private final EncryptionHelper encryptionHelper;
    private final StorPoolDefinitionRepository storPoolDfnRepo;

    @Inject
    public CtrlStorPoolApiCallHandler(
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ResponseConverter responseConverterRef,
        Provider<Peer> peerRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        @SystemContext AccessContext sysCtxRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlSecurityObjects ctrlSecurityObjects,
        EncryptionHelper encryptionHelperRef,
        StorPoolDefinitionRepository storPoolDfnRepoRef
    )
    {
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        responseConverter = responseConverterRef;
        peer = peerRef;
        peerAccCtx = peerAccCtxRef;
        sysCtx = sysCtxRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        securityObjects = ctrlSecurityObjects;
        encryptionHelper = encryptionHelperRef;
        storPoolDfnRepo = storPoolDfnRepoRef;
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
        boolean notifyStlts = false;

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

            for (Map.Entry<String, String> entry : overrideProps.entrySet())
            {
                if (entry.getKey().startsWith(ApiConsts.NAMESPC_SED + ReadOnlyProps.PATH_SEPARATOR))
                {
                    if (!storPool.getNode().getPeer(peerAccCtx.get()).isConnected(true))
                    {
                        throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                            ApiConsts.MASK_ERROR | ApiConsts.MASK_STOR_POOL | ApiConsts.FAIL_INVLD_PROP,
                            "Cannot change SED password while node not connected.",
                            true
                        ));
                    }

                    if (securityObjects.getCryptKey() == null)
                    {
                        throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                            ApiConsts.MASK_ERROR | ApiConsts.MASK_STOR_POOL | ApiConsts.FAIL_INVLD_PROP,
                            "Need master-passphrase to change SED password.",
                            true
                        ));
                    }

                    // on the fly encrypt new SED password
                    byte[] sedEncBytePassword = encryptionHelper.encrypt(entry.getValue());
                    overrideProps.put(entry.getKey(),
                        Base64.encode(sedEncBytePassword));
                }
            }

            notifyStlts = ctrlPropsHelper.fillProperties(
                apiCallRcs,
                LinStorObject.STORAGEPOOL,
                overrideProps,
                props,
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL,
                Collections.singletonList(ApiConsts.NAMESPC_SED + ReadOnlyProps.PATH_SEPARATOR)
            ) || notifyStlts;
            notifyStlts = ctrlPropsHelper.remove(
                apiCallRcs, LinStorObject.STORAGEPOOL, props, deletePropKeys, deletePropNamespaces) || notifyStlts;

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

            if (notifyStlts)
            {
                flux = ctrlSatelliteUpdateCaller.updateSatellite(storPool);
            }
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
            if (storPool.getDeviceProviderKind().equals(DeviceProviderKind.EBS_TARGET))
            {
                throw new ApiRcException(
                    ApiCallRcImpl.entryBuilder(
                        ApiConsts.FAIL_INVLD_STOR_POOL_NAME,
                            "Deletion of " + getStorPoolDescriptionInline(nodeNameStr, storPoolNameStr) +
                                " is not allowed, since you cannot re-create it."
                        )
                        .setSkipErrorReport(true)
                        .build()
                );
            }

            Collection<VlmProviderObject<Resource>> volumes = getVolumes(storPool);
            Collection<VlmProviderObject<Snapshot>> snapVlms = getSnapshotVolumes(storPool);
            if (!volumes.isEmpty() || !snapVlms.isEmpty())
            {
                StringBuilder volListSb = new StringBuilder();
                for (VlmProviderObject<Resource> vlmObj : volumes)
                {
                    Resource rsc = vlmObj.getVolume().getAbsResource();
                    volListSb.append("\n   Node name: '")
                        .append(rsc.getNode().getName().displayValue)
                        .append("', resource name: '")
                        .append(rsc.getResourceDefinition().getName().displayValue)
                        .append("', volume number: ")
                        .append(vlmObj.getVlmNr().value)
                        .append(", resource suffix: '")
                        .append(vlmObj.getRscLayerObject().getResourceNameSuffix())
                        .append("'");
                }
                for (VlmProviderObject<Snapshot> snapVlmObj : snapVlms)
                {
                    Snapshot snap = snapVlmObj.getVolume().getAbsResource();
                    volListSb.append("\n   Node name: '")
                        .append(snap.getNode().getName().displayValue)
                        .append("', resource name: '")
                        .append(snap.getResourceDefinition().getName().displayValue)
                        .append("', snapshot name: '")
                        .append(snap.getSnapshotDefinition().getName().displayValue)
                        .append("', volume number: ")
                        .append(snapVlmObj.getVlmNr().value)
                        .append(", resource suffix: '")
                        .append(snapVlmObj.getRscLayerObject().getResourceNameSuffix())
                        .append("'");
                }

                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_IN_USE,
                        String.format(
                                "The specified storage pool '%s' on node '%s' can not be deleted as " +
                                    "volumes / snapshot-volumes are still using it.",
                                storPoolNameStr,
                                nodeNameStr
                            )
                    )
                    .setDetails("Volumes / snapshot-volumes that are still using the storage pool: " + volListSb)
                    .setCorrection("Delete the listed volumes and snapshot-volumes first.")
                    .build()
                );
            }
            else
            {
                UUID storPoolUuid = storPool.getUuid(); // cache storpool uuid to avoid access deleted storpool

                final Node storPoolNode = storPool.getNode();
                StorPoolDefinition spd = getStorPoolDefinition(storPool);
                delete(storPool);
                Iterator<StorPool> spIt = spd.iterateStorPools(sysCtx);
                String spdDspName = spd.getName().displayValue;
                if (!spIt.hasNext() && // only delete SPD if we just its deleted last SP
                    // and if the name is not default*
                    !spdDspName.equalsIgnoreCase(LinStor.DISKLESS_STOR_POOL_NAME) &&
                    !spdDspName.equalsIgnoreCase(InternalApiConsts.DEFAULT_STOR_POOL_NAME))
                {
                    delete(spd);
                }
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

    private Collection<VlmProviderObject<Snapshot>> getSnapshotVolumes(StorPool storPool)
    {
        Collection<VlmProviderObject<Snapshot>> snapVlms;
        try
        {
            snapVlms = storPool.getSnapVolumes(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access the snapshot-volumes of " + getStorPoolDescriptionInline(storPool),
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL
            );
        }
        return snapVlms;
    }

    private StorPoolDefinition getStorPoolDefinition(StorPool storPoolRef)
    {
        StorPoolDefinition spd;
        try
        {
            spd = storPoolRef.getDefinition(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "accessinng definition of " + StorPoolHelper.getStorPoolDescriptionInline(storPoolRef),
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL_DFN
            );

        }
        return spd;
    }

    private void delete(StorPoolDefinition storPoolDfn)
    {
        try
        {
            StorPoolName spName = storPoolDfn.getName();
            storPoolDfn.delete(peerAccCtx.get());
            storPoolDfnRepo.remove(peerAccCtx.get(), spName);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "delete " + CtrlStorPoolDfnApiCallHandler.getStorPoolDfnDescriptionInline(storPoolDfn),
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL_DFN
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
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
