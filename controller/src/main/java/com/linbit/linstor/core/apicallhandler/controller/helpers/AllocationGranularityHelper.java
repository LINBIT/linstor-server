package com.linbit.linstor.core.apicallhandler.controller.helpers;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.data.provider.zfs.ZfsData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.linstor.utils.layer.LayerVlmUtils;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

@Singleton
public class AllocationGranularityHelper
{
    private final Provider<AccessContext> peerAccCtxProvider;

    @Inject
    public AllocationGranularityHelper(@PeerContext Provider<AccessContext> peerAccCtxProviderRef)
    {
        peerAccCtxProvider = peerAccCtxProviderRef;
    }

    public void updateIfNeeded(ResourceDefinition rscDfnRef, boolean includeStorPoolCheckRef)
        throws AccessDeniedException, DatabaseException
    {
        updateIfNeeded(peerAccCtxProvider.get(), rscDfnRef, includeStorPoolCheckRef);
    }

    /**
     * Usually LINSTOR creates the volumes itself, but if the volume already existed (i.e. was imported into LINSTOR),
     * the volume (especially on ZFS) might have an unexpected allocation granularity. This method checks if the
     * volume-definition's property is large enough so new resources can be added as well as future resize actions can
     * succeed.
     *
     * @param accCtxRef The accessContext that should be used.
     * @param rscDfnRef All volume definitions of the given resource definition will be checked
     * @param includeStorPoolCheckRef If set to true, this method also checks for the storage pool properties
     */
    public void updateIfNeeded(AccessContext accCtxRef, ResourceDefinition rscDfnRef, boolean includeStorPoolCheckRef)
        throws AccessDeniedException, DatabaseException
    {
        Map<VolumeDefinition, TreeSet<Long>> allocationGranularitiesByVlmDfnMap = new HashMap<>();

        Iterator<Resource> rscIt = rscDfnRef.iterateResource(accCtxRef);
        while (rscIt.hasNext())
        {
            Resource rsc = rscIt.next();

            Set<AbsRscLayerObject<Resource>> storRscDataSet = LayerRscUtils.getRscDataByLayer(
                rsc.getLayerData(accCtxRef),
                DeviceLayerKind.STORAGE
            );

            for (AbsRscLayerObject<Resource> storRscData : storRscDataSet)
            {
                for (VlmProviderObject<Resource> storVlmData : storRscData.getVlmLayerObjects().values())
                {
                    // currently only StoragePools and ZfsData might contribute to the set of allocationGranularities
                    if (includeStorPoolCheckRef)
                    {
                        addStorPoolAllocGrans(accCtxRef, allocationGranularitiesByVlmDfnMap, storVlmData);
                    }
                    addVolumeSpecificAllocGran(allocationGranularitiesByVlmDfnMap, storVlmData);
                }
            }
        }

        for (Map.Entry<VolumeDefinition, TreeSet<Long>> entry : allocationGranularitiesByVlmDfnMap.entrySet())
        {
            adjustVlmDfnPropIfNeeded(accCtxRef, entry.getKey(), entry.getValue());
        }
    }

    private void addStorPoolAllocGrans(
        AccessContext accCtxRef,
        Map<VolumeDefinition, TreeSet<Long>> allocationGranularitiesByVlmDfnMap,
        VlmProviderObject<Resource> storVlmData
    )
        throws AccessDeniedException
    {
        Set<StorPool> storPoolSet = LayerVlmUtils.getStorPoolSet(storVlmData, accCtxRef);
        VolumeDefinition vlmDfn = storVlmData.getVolume().getVolumeDefinition();
        for (StorPool sp : storPoolSet)
        {
            ReadOnlyProps spProps = sp.getProps(accCtxRef);
            @Nullable String spPropValue = spProps.getProp(
                InternalApiConsts.ALLOCATION_GRANULARITY,
                StorageConstants.NAMESPACE_INTERNAL
            );

            if (spPropValue != null)
            {
                allocationGranularitiesByVlmDfnMap.computeIfAbsent(vlmDfn, ignore -> new TreeSet<>())
                    .add(Long.parseLong(spPropValue));
            }
        }
    }

    private void addVolumeSpecificAllocGran(
        Map<VolumeDefinition, TreeSet<Long>> allocationGranularitiesByVlmDfnMap,
        VlmProviderObject<Resource> storVlmData
    )
    {
        if (storVlmData instanceof ZfsData)
        {
            ZfsData<Resource> zfsVlmData = (ZfsData<Resource>) storVlmData;
            @Nullable Long zfsVlmExtentSize = zfsVlmData.getExtentSize();
            if (zfsVlmExtentSize != null)
            {
                allocationGranularitiesByVlmDfnMap.computeIfAbsent(
                    storVlmData.getVolume().getVolumeDefinition(),
                    ignore -> new TreeSet<>()
                )
                    .add(zfsVlmExtentSize);
            }
        }
    }

    private void adjustVlmDfnPropIfNeeded(
        AccessContext accCtxRef,
        VolumeDefinition vlmDfnRef,
        TreeSet<Long> allocGranSetRef
    )
        throws AccessDeniedException, DatabaseException
    {
        if (!allocGranSetRef.isEmpty())
        {
            Props vlmDfnProps = vlmDfnRef.getProps(accCtxRef);
            @Nullable String vlmDfnAllocGranStr = vlmDfnProps.getProp(
                InternalApiConsts.ALLOCATION_GRANULARITY,
                StorageConstants.NAMESPACE_INTERNAL
            );

            long vlmDfnAllocGran = vlmDfnAllocGranStr == null ? -1 : Long.parseLong(vlmDfnAllocGranStr);
            long largestAllocGran = allocGranSetRef.descendingIterator().next();

            if (vlmDfnAllocGran < largestAllocGran)
            {
                try
                {
                    vlmDfnProps.setProp(
                        InternalApiConsts.ALLOCATION_GRANULARITY,
                        Long.toString(largestAllocGran),
                        StorageConstants.NAMESPACE_INTERNAL
                    );
                }
                catch (InvalidKeyException | InvalidValueException exc)
                {
                    throw new ImplementationError(exc);
                }
            }
        }
    }
}
