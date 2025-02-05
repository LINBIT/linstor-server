package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcWith;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.api.pojo.AutoSelectFilterPojo;
import com.linbit.linstor.api.pojo.MaxVlmSizeCandidatePojo;
import com.linbit.linstor.api.pojo.builder.AutoSelectFilterBuilder;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.Autoplacer;
import com.linbit.linstor.core.apis.StorPoolDefinitionApi;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.repository.StorPoolDefinitionRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.utils.ComparatorUtils;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlQueryMaxVlmSizeHelper
{
    private final AccessContext apiCtx;
    private final Autoplacer autoplacer;
    private final StorPoolDefinitionRepository storPoolDfnRepo;
    private final SystemConfRepository sysCfgRepo;

    @Inject
    public CtrlQueryMaxVlmSizeHelper(
        @ApiContext AccessContext apiCtxRef,
        Autoplacer autoplacerRef,
        StorPoolDefinitionRepository storPoolDfnRepoRef,
        SystemConfRepository sysCfgRepoRef
    )
    {
        apiCtx = apiCtxRef;
        autoplacer = autoplacerRef;
        storPoolDfnRepo = storPoolDfnRepoRef;
        sysCfgRepo = sysCfgRepoRef;
    }

    public Flux<ApiCallRcWith<List<MaxVlmSizeCandidatePojo>>> queryMaxVlmSize(
        AutoSelectFilterApi selectFilter,
        @Nullable ResourceDefinition rscDfnRef,
        int size,
        @Nullable Map<StorPool.Key, Long> thinFreeCapacities
    )
    {
        /*
         * This is a hack.
         *
         * The problem is that the old autoplacer used to select only storage pool with a
         * common storage pool definition (i.e. only one storage pool name and a list of node-names
         * were sufficient to describe the selection).
         *
         * The new autoplacer got rid of this limitation, now allowing storage pools with different
         * storage pool definitions in the selection.
         *
         * The query-max-volume-size still depends on this grouped storage pool name property of
         * the candidates.
         *
         * In order to provide compatibility, we therefore (although its pretty ugly) we run an
         * autoplacer for each storage pool definition and filter by its name in order to simulate
         * the old autoplacers behaviour.
         */

        List<MaxVlmSizeCandidatePojo> candidatesWithCapacity = new ArrayList<>();

        List<String> storPoolNameList = selectFilter.getStorPoolNameList();
        if (storPoolNameList == null || storPoolNameList.isEmpty())
        {
            storPoolNameList = getStorPoolNamesAsStringPrivileged();
        }

        final ReadOnlyProps ctrlProps = getCtrlPropsPrivileged();

        for (String storPoolNameStr : storPoolNameList)
        {
            AutoSelectFilterPojo currentFilter = AutoSelectFilterPojo.merge(
                new AutoSelectFilterBuilder()
                    // only consider this one storpoolname
                    .setStorPoolNameList(Collections.singletonList(storPoolNameStr))
                    .build(),
                selectFilter // copy the rest from selectFilter
            );

            Set<StorPool> selectedStorPoolSet = autoplacer.autoPlace(currentFilter, rscDfnRef, size);
            if (selectedStorPoolSet != null)
            {
                Set<StorPoolDefinition> spDfnSet = new HashSet<>();
                boolean allThin = true;
                List<String> nodeNameList = new ArrayList<>();
                long lowestFreeSpace = Long.MAX_VALUE;

                for (StorPool sp : selectedStorPoolSet)
                {
                    spDfnSet.add(getStorPoolDfnPrivileged(sp));
                    allThin &= sp.getDeviceProviderKind().usesThinProvisioning();

                    nodeNameList.add(sp.getNode().getName().displayValue);

                    Optional<Long> freeCapacityCurrentEstimation =
                        FreeCapacityAutoPoolSelectorUtils.getFreeCapacityCurrentEstimationPrivileged(
                            apiCtx,
                            thinFreeCapacities,
                            sp,
                            ctrlProps,
                            false
                        );
                    if (freeCapacityCurrentEstimation.isPresent())
                    {
                        Long spFreeSpace = freeCapacityCurrentEstimation.get();
                        if (lowestFreeSpace > spFreeSpace)
                        {
                            lowestFreeSpace = spFreeSpace;
                        }
                    }
                }

                if (spDfnSet.size() > 1)
                {
                    throw new ImplementationError("QVMS compatibilty failed");
                }

                candidatesWithCapacity.add(
                    new MaxVlmSizeCandidatePojo(
                        getStorPoolDfnApiDataPrivileged(spDfnSet.iterator().next()),
                        allThin,
                        nodeNameList,
                        lowestFreeSpace
                    )
                );
            }
        }
        return Flux.just(makeResponse(candidatesWithCapacity));
    }

    private List<String> getStorPoolNamesAsStringPrivileged()
    {
        List<String> storPoolNamesStr = new ArrayList<>();
        try
        {
            Set<StorPoolName> storPoolNames = storPoolDfnRepo.getMapForView(apiCtx).keySet();
            for (StorPoolName spName : storPoolNames)
            {
                storPoolNamesStr.add(spName.displayValue);
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return storPoolNamesStr;
    }

    private StorPoolDefinition getStorPoolDfnPrivileged(StorPool storPool)
    {
        StorPoolDefinition storPoolDfn;
        try
        {
            storPoolDfn = storPool.getDefinition(apiCtx);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return storPoolDfn;
    }

    private StorPoolDefinitionApi getStorPoolDfnApiDataPrivileged(StorPoolDefinition storPoolDfn)
    {
        StorPoolDefinitionApi apiData;
        try
        {
            apiData = storPoolDfn.getApiData(apiCtx);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return apiData;
    }

    private ReadOnlyProps getCtrlPropsPrivileged()
    {
        try
        {
            return sysCfgRepo.getCtrlConfForView(apiCtx);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private ApiCallRcWith<List<MaxVlmSizeCandidatePojo>> makeResponse(List<MaxVlmSizeCandidatePojo> candidates)
    {
        ApiCallRc apirc = null;
        if (candidates.isEmpty())
        {
            apirc = ApiCallRcImpl.singletonApiCallRc(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.MASK_ERROR | ApiConsts.FAIL_NOT_ENOUGH_NODES,
                    "Not enough nodes"
                )
            );
        }
        else
        {
            candidates.sort(
                ComparatorUtils.comparingWithComparator(
                    MaxVlmSizeCandidatePojo::getStorPoolDfnApi,
                    Comparator.comparing(StorPoolDefinitionApi::getName)
                )
            );
        }
        return new ApiCallRcWith<>(apirc, candidates);
    }
}
