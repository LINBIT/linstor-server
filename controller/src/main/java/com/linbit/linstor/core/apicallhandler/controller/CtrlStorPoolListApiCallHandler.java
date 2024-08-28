package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.LinStorException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.DecryptionHelper;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.core.CtrlSecurityObjects;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ResponseUtils;
import com.linbit.linstor.core.apis.StorPoolApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.repository.StorPoolDefinitionRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;
import com.linbit.utils.Base64;

import static com.linbit.locks.LockGuardFactory.LockObj.STOR_POOL_DFN_MAP;
import static com.linbit.locks.LockGuardFactory.LockType.READ;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.MDC;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

import static java.util.stream.Collectors.toList;

@Singleton
public class CtrlStorPoolListApiCallHandler
{
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final FreeCapacityFetcher freeCapacityFetcher;
    private final StorPoolDefinitionRepository storPoolDefinitionRepository;
    private final Provider<AccessContext> peerAccCtx;
    private final DecryptionHelper decryptionHelper;
    private final CtrlSecurityObjects secObjs;
    private final SystemConfRepository sysCfgRepo;
    private final AccessContext sysCtx;

    @Inject
    public CtrlStorPoolListApiCallHandler(
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        FreeCapacityFetcher freeCapacityFetcherRef,
        StorPoolDefinitionRepository storPoolDefinitionRepositoryRef,
        @SystemContext AccessContext sysCtxRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        DecryptionHelper decryptionHelperRef,
        CtrlSecurityObjects secObjsRef,
        SystemConfRepository sysCfgRepoRef
    )
    {
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        freeCapacityFetcher = freeCapacityFetcherRef;
        storPoolDefinitionRepository = storPoolDefinitionRepositoryRef;
        sysCtx = sysCtxRef;
        peerAccCtx = peerAccCtxRef;
        decryptionHelper = decryptionHelperRef;
        secObjs = secObjsRef;
        sysCfgRepo = sysCfgRepoRef;
    }

    public Flux<List<StorPoolApi>> listStorPools(
        List<String> nodeNames,
        List<String> storPoolNames,
        List<String> propFilters,
        boolean fromCache
    )
    {
        Flux<List<StorPoolApi>> flux;
        final Set<StorPoolName> storPoolsFilter = storPoolNames.stream()
            .map(LinstorParsingUtils::asStorPoolName)
            .collect(Collectors.toSet());
        final Set<NodeName> nodesFilter = nodeNames.stream()
            .map(LinstorParsingUtils::asNodeName)
            .collect(Collectors.toSet());
        if (fromCache)
        {
            flux = scopeRunner.fluxInTransactionlessScope(
                "Assemble storage pool list from Cache",
                lockGuardFactory.buildDeferred(READ, STOR_POOL_DFN_MAP),
                () -> Flux.just(assembleList(nodesFilter, storPoolsFilter, propFilters, null))
            );
        }
        else
        {
            flux = freeCapacityFetcher.fetchThinFreeSpaceInfo(nodesFilter)
                .flatMapMany(
                    freeCapacityAnswers -> scopeRunner.fluxInTransactionlessScope(
                        "Assemble storage pool list",
                        lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.STOR_POOL_DFN_MAP),
                        () -> Flux.just(assembleList(nodesFilter, storPoolsFilter, propFilters, freeCapacityAnswers)),
                        MDC.getCopyOfContextMap()
                    )
                );
        }
        return flux;
    }

    public List<StorPoolApi> listStorPoolsCached(
        List<String> nodeNames,
        List<String> storPoolNames,
        List<String> propFilters
    )
    {
        final Set<StorPoolName> storPoolsFilter =
            storPoolNames.stream().map(LinstorParsingUtils::asStorPoolName).collect(Collectors.toSet());
        final Set<NodeName> nodesFilter =
            nodeNames.stream().map(LinstorParsingUtils::asNodeName).collect(Collectors.toSet());

        try (LockGuard ignored = lockGuardFactory.build(READ, STOR_POOL_DFN_MAP))
        {
            return assembleList(nodesFilter, storPoolsFilter, propFilters, null);
        }
    }

    /**
     * Change listed props to only show user allowed things
     *
     * Currently, SED passwords will be masked until the master-passphrase is entered.
     *
     * @param props
     */
    private void patchStorPoolProps(Map<String, String> props)
    {
        for (String key : props.keySet())
        {
            if (key.startsWith(ApiConsts.NAMESPC_SED + ReadOnlyProps.PATH_SEPARATOR))
            {
                byte[] masterKey = secObjs.getCryptKey();
                if (masterKey == null)
                {
                    props.put(key, "***MASTER-PASSPHRASE-REQUIRED***");
                }
                else
                {
                    String sedEncPassword = props.get(key);
                    byte[] sedByteEncPassword = Base64.decode(sedEncPassword);
                    try
                    {
                        byte[] decryptedKey = decryptionHelper.decrypt(masterKey, sedByteEncPassword);
                        String sedPassword = new String(decryptedKey);
                        props.put(key, sedPassword);
                    }
                    catch (LinStorException linExc)
                    {
                        props.put(key, "***ERROR-DECRYPTING-PASSWORD***");
                    }
                }
            }
        }
    }

    private List<StorPoolApi> assembleList(
        Set<NodeName> nodesFilter,
        Set<StorPoolName> storPoolsFilter,
        List<String> propFilters,
        @Nullable Map<StorPool.Key, Tuple2<SpaceInfo, List<ApiCallRc>>> freeCapacityAnswers
    )
    {
        ArrayList<StorPoolApi> storPools = new ArrayList<>();
        try
        {
            ReadOnlyProps ctrlProps = sysCfgRepo.getCtrlConfForView(peerAccCtx.get());
            storPoolDefinitionRepository.getMapForView(peerAccCtx.get()).values().stream()
                .filter(
                    storPoolDfn -> storPoolsFilter.isEmpty() ||
                    storPoolsFilter.contains(storPoolDfn.getName())
                )
                .forEach(
                    storPoolDfn ->
                    {
                        try
                        {
                            for (StorPool storPool : storPoolDfn.streamStorPools(peerAccCtx.get())
                                .filter(storPool -> nodesFilter.isEmpty() ||
                                    nodesFilter.contains(storPool.getNode().getName()))
                                .collect(toList()))
                            {
                                ReadOnlyProps props = storPool.getProps(peerAccCtx.get());
                                if (props.contains(propFilters))
                                {
                                    Long freeCapacity;
                                    Long totalCapacity;

                                    final Tuple2<SpaceInfo, List<ApiCallRc>> storageInfo = freeCapacityAnswers != null ?
                                        freeCapacityAnswers.get(new StorPool.Key(storPool)) : null;

                                    storPool.clearReports();
                                    Peer peer = storPool.getNode().getPeer(peerAccCtx.get());
                                    if (peer == null || !peer.isOnline())
                                    {
                                        freeCapacity = null;
                                        totalCapacity = null;
                                        storPool.addReports(
                                            new ApiCallRcImpl(
                                                ResponseUtils.makeNotConnectedWarning(storPool.getNode().getName())
                                            )
                                        );
                                    }
                                    else
                                    if (storageInfo == null)
                                    {
                                        freeCapacity = storPool.getFreeSpaceTracker()
                                            .getFreeCapacityLastUpdated(peerAccCtx.get()).orElse(null);
                                        totalCapacity = storPool.getFreeSpaceTracker()
                                            .getTotalCapacity(peerAccCtx.get()).orElse(null);
                                    }
                                    else
                                    {
                                        SpaceInfo spaceInfo = storageInfo.getT1();
                                        for (ApiCallRc apiCallRc : storageInfo.getT2())
                                        {
                                            storPool.addReports(apiCallRc);
                                        }

                                        freeCapacity = spaceInfo.freeCapacity;
                                        totalCapacity = spaceInfo.totalCapacity;
                                    }

                                    // fullSyncId and updateId null, as they are not going to be serialized anyway
                                    StorPoolApi apiData = storPool.getApiData(
                                        totalCapacity,
                                        freeCapacity,
                                        peerAccCtx.get(),
                                        null,
                                        null,
                                        FreeCapacityAutoPoolSelectorUtils
                                            .getFreeCapacityOversubscriptionRatioPrivileged(
                                                sysCtx,
                                                storPool,
                                                ctrlProps
                                            ),
                                        FreeCapacityAutoPoolSelectorUtils
                                            .getTotalCapacityOversubscriptionRatioPrivileged(
                                                sysCtx,
                                                storPool,
                                                ctrlProps
                                            )
                                    );
                                    patchStorPoolProps(apiData.getStorPoolProps());
                                    storPools.add(apiData);
                                }
                            }
                        }
                        catch (AccessDeniedException accDeniedExc)
                        {
                            // don't add storpooldfn without access
                        }
                    }
                );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "view storage pool definitions",
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL_DFN
            );
        }

        return storPools;
    }
}
