package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.AutoSelectFilterPojo;
import com.linbit.linstor.api.pojo.builder.AutoSelectFilterBuilder;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.Autoplacer;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import reactor.core.publisher.Flux;

/**
 * Helper for rebalancing resources after clone or snapshot restore.
 *
 * When resources are cloned or restored from snapshot, replicas are placed on the
 * same nodes as the source. This helper migrates replicas to the autoplacer-optimal
 * nodes via sequential migrate-disk (toggle-disk with MigrateFrom) calls.
 */
@Singleton
public class CtrlRscRebalanceHelper
{
    private final ErrorReporter errorReporter;
    private final Autoplacer autoplacer;
    private final CtrlRscToggleDiskApiCallHandler rscToggleDiskHelper;
    private final Provider<AccessContext> peerAccCtx;

    @Inject
    public CtrlRscRebalanceHelper(
        ErrorReporter errorReporterRef,
        Autoplacer autoplacerRef,
        CtrlRscToggleDiskApiCallHandler rscToggleDiskHelperRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef
    )
    {
        errorReporter = errorReporterRef;
        autoplacer = autoplacerRef;
        rscToggleDiskHelper = rscToggleDiskHelperRef;
        peerAccCtx = peerAccCtxRef;
    }

    /**
     * Trigger rebalance for the given resource definition.
     *
     * Computes optimal placement via autoplacer, then sequentially calls
     * migrate-disk for each misplaced replica.
     *
     * @param rscDfn the resource definition to rebalance
     * @return Flux of ApiCallRc from the migrate-disk operations
     */
    public Flux<ApiCallRc> trigger(ResourceDefinition rscDfn)
    {
        try
        {
            AccessContext accCtx = peerAccCtx.get();
            String rscName = rscDfn.getName().displayValue;

            List<Resource> currentDiskful = rscDfn.getDiskfulResources(accCtx);
            if (currentDiskful.isEmpty())
            {
                return Flux.empty();
            }

            int replicaCount = currentDiskful.size();
            long rscSize = CtrlRscAutoPlaceApiCallHandler.calculateResourceDefinitionSize(rscDfn, accCtx);

            // Query autoplacer for optimal fresh placement (null rscDfn = ignore existing replicas)
            AutoSelectFilterPojo selectFilter = AutoSelectFilterPojo.merge(
                new AutoSelectFilterBuilder()
                    .setPlaceCount(replicaCount)
                    .build(),
                rscDfn.getResourceGroup().getAutoPlaceConfig().getApiData()
            );

            @Nullable Set<StorPool> optimalPools = autoplacer.autoPlace(selectFilter, null, rscSize);
            if (optimalPools == null)
            {
                errorReporter.logWarning(
                    "Rebalance: autoplacer could not find optimal placement for '%s', skipping rebalance",
                    rscName
                );
                return Flux.empty();
            }

            Set<String> optimalNodeNames = optimalPools.stream()
                .map(sp -> sp.getNode().getName().displayValue)
                .collect(Collectors.toSet());

            Set<String> currentNodeNames = currentDiskful.stream()
                .map(rsc -> rsc.getNode().getName().displayValue)
                .collect(Collectors.toSet());

            // Compute migration pairs: source nodes that are not optimal -> target nodes that are not current
            List<String> nodesToRemove = currentNodeNames.stream()
                .filter(n -> !optimalNodeNames.contains(n))
                .collect(Collectors.toList());

            List<String> nodesToAdd = optimalNodeNames.stream()
                .filter(n -> !currentNodeNames.contains(n))
                .collect(Collectors.toList());

            int pairCount = Math.min(nodesToRemove.size(), nodesToAdd.size());
            if (pairCount == 0)
            {
                errorReporter.logInfo(
                    "Rebalance: '%s' is already on optimal nodes, no migration needed",
                    rscName
                );
                return Flux.empty();
            }

            // Build migration pairs: source -> target (with target storage pool name)
            Map<StorPool, String> targetPoolToSource = new LinkedHashMap<>();
            for (int i = 0; i < pairCount; i++)
            {
                String targetNodeName = nodesToAdd.get(i);
                String sourceNodeName = nodesToRemove.get(i);

                // Find the storage pool for target node from autoplacer results
                StorPool targetPool = optimalPools.stream()
                    .filter(sp -> sp.getNode().getName().displayValue.equals(targetNodeName))
                    .findFirst()
                    .orElse(null);

                if (targetPool != null)
                {
                    targetPoolToSource.put(targetPool, sourceNodeName);
                }
            }

            if (targetPoolToSource.isEmpty())
            {
                return Flux.empty();
            }

            List<String> migrationSummary = new ArrayList<>();
            for (Map.Entry<StorPool, String> entry : targetPoolToSource.entrySet())
            {
                migrationSummary.add(
                    entry.getValue() + " -> " + entry.getKey().getNode().getName().displayValue
                );
            }
            errorReporter.logInfo(
                "Rebalance: '%s' scheduling %d migration(s): %s",
                rscName,
                pairCount,
                String.join(", ", migrationSummary)
            );

            // Chain sequential migrate-disk calls via toggle-disk
            Flux<ApiCallRc> migrationFlux = Flux.empty();
            for (Map.Entry<StorPool, String> entry : targetPoolToSource.entrySet())
            {
                StorPool targetPool = entry.getKey();
                String sourceNodeName = entry.getValue();
                String targetNodeName = targetPool.getNode().getName().displayValue;
                String storPoolName = targetPool.getName().displayValue;

                migrationFlux = migrationFlux.concatWith(
                    rscToggleDiskHelper.resourceToggleDisk(
                        targetNodeName,
                        rscName,
                        storPoolName,
                        sourceNodeName,   // migrateFrom
                        null,             // layerList
                        false,            // removeDisk = false (adding disk)
                        Resource.DiskfulBy.USER
                    )
                );
            }

            return Flux.<ApiCallRc>just(
                ApiCallRcImpl.singleApiCallRc(
                    ApiConsts.MASK_INFO,
                    "Rebalance: scheduling " + pairCount + " migration(s) for resource '" + rscName + "'"
                )
            ).concatWith(migrationFlux);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "rebalancing resource",
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
    }
}
