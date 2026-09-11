package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.DecryptionHelper;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.core.CtrlSecurityObjects;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.response.ResponseUtils;
import com.linbit.linstor.core.apis.StorPoolApi;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.repository.StorPoolDefinitionRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;
import com.linbit.utils.Base64;
import com.linbit.utils.RegexMatcher;

import static com.linbit.locks.LockGuardFactory.LockObj.STOR_POOL_DFN_MAP;
import static com.linbit.locks.LockGuardFactory.LockType.READ;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

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
    private final DecryptionHelper decryptionHelper;
    private final CtrlSecurityObjects secObjs;
    private final SystemConfRepository sysCfgRepo;

    @Inject
    public CtrlStorPoolListApiCallHandler(
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        FreeCapacityFetcher freeCapacityFetcherRef,
        StorPoolDefinitionRepository storPoolDefinitionRepositoryRef,
        DecryptionHelper decryptionHelperRef,
        CtrlSecurityObjects secObjsRef,
        SystemConfRepository sysCfgRepoRef
    )
    {
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        freeCapacityFetcher = freeCapacityFetcherRef;
        storPoolDefinitionRepository = storPoolDefinitionRepositoryRef;
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
        final List<Pattern> storPoolsFilter = RegexMatcher.compileAll(storPoolNames, true);
        final List<Pattern> nodesFilter = RegexMatcher.compileAll(nodeNames, true);
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
        final List<Pattern> storPoolsFilter = RegexMatcher.compileAll(storPoolNames, true);
        final List<Pattern> nodesFilter = RegexMatcher.compileAll(nodeNames, true);

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
     */
    private void patchStorPoolProps(Map<String, String> props)
    {
        for (Map.Entry<String, String> entry : props.entrySet())
        {
            if (entry.getKey().startsWith(ApiConsts.NAMESPC_SED + ReadOnlyProps.PATH_SEPARATOR))
            {
                byte[] masterKey = secObjs.getCryptKey();
                if (masterKey == null)
                {
                    entry.setValue("***MASTER-PASSPHRASE-REQUIRED***");
                }
                else
                {
                    byte[] sedByteEncPassword = Base64.decode(entry.getValue());
                    try
                    {
                        byte[] decryptedKey = decryptionHelper.decrypt(masterKey, sedByteEncPassword);
                        String sedPassword = new String(decryptedKey, StandardCharsets.UTF_8);
                        entry.setValue(sedPassword);
                    }
                    catch (LinStorException linExc)
                    {
                        entry.setValue("***ERROR-DECRYPTING-PASSWORD***");
                    }
                }
            }
        }
    }

    private List<StorPoolApi> assembleList(
        List<Pattern> nodesFilter,
        List<Pattern> storPoolsFilter,
        List<String> propFilters,
        @Nullable Map<StorPool.Key, Tuple2<SpaceInfo, List<ApiCallRc>>> freeCapacityAnswers
    )
    {
        ArrayList<StorPoolApi> storPools = new ArrayList<>();
        ReadOnlyProps ctrlProps = sysCfgRepo.getCtrlConfForView();
        storPoolDefinitionRepository.getMapForView().values().stream()
            .filter(
                storPoolDfn -> RegexMatcher.matchesAny(storPoolsFilter, storPoolDfn.getName().displayValue)
            )
            .forEach(
                storPoolDfn ->
                {
                    for (StorPool storPool : storPoolDfn.streamStorPools()
                        .filter(storPool -> RegexMatcher.matchesAny(
                            nodesFilter, storPool.getNode().getName().displayValue))
                        .collect(toList()))
                    {
                        ReadOnlyProps props = storPool.getProps();
                        if (props.contains(propFilters))
                        {
                            Long freeCapacity;
                            Long totalCapacity;

                            final Tuple2<SpaceInfo, List<ApiCallRc>> storageInfo = freeCapacityAnswers != null ?
                                freeCapacityAnswers.get(new StorPool.Key(storPool)) : null;

                            storPool.clearReports();
                            Peer peer = storPool.getNode().getPeer();
                            if (!peer.isOnline())
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
                                    .getFreeCapacityLastUpdated().orElse(null);
                                totalCapacity = storPool.getFreeSpaceTracker()
                                    .getTotalCapacity().orElse(null);
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
                                null,
                                null,
                                FreeCapacityAutoPoolSelectorUtils
                                    .getFreeCapacityOversubscriptionRatioPrivileged(
                                        storPool,
                                        ctrlProps
                                    ),
                                FreeCapacityAutoPoolSelectorUtils
                                    .getTotalCapacityOversubscriptionRatioPrivileged(
                                        storPool,
                                        ctrlProps
                                    )
                            );
                            patchStorPoolProps(apiData.getStorPoolProps());
                            storPools.add(apiData);
                        }
                    }
                }
            );

        return storPools;
    }
}
