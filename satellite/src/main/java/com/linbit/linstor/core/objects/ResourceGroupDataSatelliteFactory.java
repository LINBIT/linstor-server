package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AutoSelectorConfigData;
import com.linbit.linstor.core.objects.ResourceGroupData;
import com.linbit.linstor.core.objects.VolumeGroup;
import com.linbit.linstor.core.objects.VolumeGroupData;
import com.linbit.linstor.core.objects.ResourceGroup.RscGrpApi;
import com.linbit.linstor.core.objects.VolumeGroup.VlmGrpApi;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.ResourceGroupDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeGroupDataDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;

@Singleton
public class ResourceGroupDataSatelliteFactory
{
    private final AccessContext sysCtx;
    private final ResourceGroupDataDatabaseDriver rscGrpDriver;
    private final VolumeGroupDataDatabaseDriver vlmGrpDriver;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final CoreModule.ResourceGroupMap rscGrpMap;

    @Inject
    public ResourceGroupDataSatelliteFactory(
        @SystemContext AccessContext sysCtxRef,
        ResourceGroupDataDatabaseDriver rscGrpDriverRef,
        VolumeGroupDataDatabaseDriver vlmGrpDriverRef,
        ObjectProtectionFactory objectProtectionFactoryRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        CoreModule.ResourceGroupMap rscGrpMapRef
    )
    {
        sysCtx = sysCtxRef;
        rscGrpDriver = rscGrpDriverRef;
        vlmGrpDriver = vlmGrpDriverRef;
        objectProtectionFactory = objectProtectionFactoryRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        rscGrpMap = rscGrpMapRef;
    }

    public ResourceGroupData getInstanceSatellite(
        ResourceGroupName rscGrpName,
        @Nullable String description,
        @Nullable List<DeviceLayerKind> layerStackRef,
        @Nullable Integer autoPlaceReplicaCountRef,
        @Nullable String autoPlaceStorPoolNameRef,
        @Nullable List<String> autoPlaceDoNotPlaceWithRscListRef,
        @Nullable String autoPlaceDoNotPlaceWithRscRegexRef,
        @Nullable List<String> autoPlaceReplicasOnSameListRef,
        @Nullable List<String> autoPlaceReplicasOnDifferentListRef,
        @Nullable List<DeviceProviderKind> autoPlaceAllowedProviderListRef
    )
        throws DatabaseException, AccessDeniedException
    {
        ResourceGroupData rscGrp = (ResourceGroupData) rscGrpMap.get(rscGrpName);

        if (rscGrp == null)
        {
            rscGrp = new ResourceGroupData(
                UUID.randomUUID(),
                objectProtectionFactory.getInstance(
                    sysCtx,
                    ObjectProtection.buildPath(rscGrpName),
                    true
                ),
                rscGrpName,
                description,
                new ArrayList<>(layerStackRef),
                autoPlaceReplicaCountRef,
                autoPlaceStorPoolNameRef,
                new ArrayList<>(autoPlaceDoNotPlaceWithRscListRef),
                autoPlaceDoNotPlaceWithRscRegexRef,
                new ArrayList<>(autoPlaceReplicasOnSameListRef),
                new ArrayList<>(autoPlaceReplicasOnDifferentListRef),
                new ArrayList<>(autoPlaceAllowedProviderListRef),
                new TreeMap<>(),
                new TreeMap<>(),
                rscGrpDriver,
                propsContainerFactory,
                transObjFactory,
                transMgrProvider
            );
            rscGrpMap.put(rscGrpName, rscGrp);
        }
        return rscGrp;
    }

    public ResourceGroupData merge(RscGrpApi rscGrpApiRef)
        throws AccessDeniedException, DatabaseException
    {
        ResourceGroupName rscGrpName;
        ResourceGroupData rscGrp;
        try
        {
            rscGrpName = new ResourceGroupName(rscGrpApiRef.getName());

            rscGrp = (ResourceGroupData) rscGrpMap.get(rscGrpName);

            if (rscGrp == null)
            {
                AutoSelectFilterApi autoSelectFilter = rscGrpApiRef.getAutoSelectFilter();

                TreeMap<VolumeNumber, VolumeGroup> vlmGrpMap = new TreeMap<>();
                rscGrp = new ResourceGroupData(
                    UUID.randomUUID(),
                    objectProtectionFactory.getInstance(
                        sysCtx,
                        ObjectProtection.buildPath(rscGrpName),
                        true
                    ),
                    rscGrpName,
                    rscGrpApiRef.getDescription(),
                    rscGrpApiRef.getLayerStack(),
                    autoSelectFilter.getReplicaCount(),
                    autoSelectFilter.getStorPoolNameStr(),
                    autoSelectFilter.getDoNotPlaceWithRscList(),
                    autoSelectFilter.getDoNotPlaceWithRscRegex(),
                    autoSelectFilter.getReplicasOnSameList(),
                    autoSelectFilter.getReplicasOnDifferentList(),
                    autoSelectFilter.getProviderList(),
                    vlmGrpMap,
                    new TreeMap<>(),
                    rscGrpDriver,
                    propsContainerFactory,
                    transObjFactory,
                    transMgrProvider
                );

                for (VlmGrpApi vlmGrpApi : rscGrpApiRef.getVlmGrpList())
                {
                    VolumeNumber vlmNr = new VolumeNumber(vlmGrpApi.getVolumeNr());
                    VolumeGroupData vlmGrp = new VolumeGroupData(
                        vlmGrpApi.getUUID(),
                        rscGrp,
                        vlmNr,
                        vlmGrpDriver,
                        propsContainerFactory,
                        transObjFactory,
                        transMgrProvider
                    );
                    vlmGrpMap.put(vlmNr, vlmGrp);
                }
            }
            else
            {
                List<DeviceLayerKind> layerStack = rscGrp.getLayerStack(sysCtx);
                if (!layerStack.equals(rscGrpApiRef.getLayerStack()))
                {
                    layerStack.clear();
                    layerStack.addAll(rscGrpApiRef.getLayerStack());
                }
                if (!rscGrp.getDescription(sysCtx).equals(rscGrpApiRef.getDescription()))
                {
                    rscGrp.setDescription(sysCtx, rscGrpApiRef.getDescription());
                }
                AutoSelectorConfigData autoPlaceConfig = (AutoSelectorConfigData) rscGrp.getAutoPlaceConfig();

                autoPlaceConfig.applyChanges(rscGrpApiRef.getAutoSelectFilter());

                TreeSet<VolumeNumber> vlmGrpsToDelete = new TreeSet<>(
                    rscGrp.streamVolumeGroups(sysCtx)
                        .map(VolumeGroup::getVolumeNumber)
                        .collect(Collectors.toSet())
                );
                for (VlmGrpApi vlmGrpApi : rscGrpApiRef.getVlmGrpList())
                {
                    VolumeNumber vlmNr = new VolumeNumber(vlmGrpApi.getVolumeNr());
                    vlmGrpsToDelete.remove(vlmNr);
                    VolumeGroupData vlmGrp = (VolumeGroupData) rscGrp.getVolumeGroup(sysCtx, vlmNr);

                    if (vlmGrp == null)
                    {
                        vlmGrp = new VolumeGroupData(
                            vlmGrpApi.getUUID(),
                            rscGrp,
                            vlmNr,
                            vlmGrpDriver,
                            propsContainerFactory,
                            transObjFactory,
                            transMgrProvider
                        );
                        rscGrp.putVolumeGroup(sysCtx, vlmGrp);
                    }
                    Map<String, String> vlmGrpPropsMap = vlmGrp.getProps(sysCtx).map();
                    vlmGrpPropsMap.clear();
                    vlmGrpPropsMap.putAll(vlmGrpApi.getProps());
                }
                for (VolumeNumber vlmNr : vlmGrpsToDelete)
                {
                    rscGrp.deleteVolumeGroup(sysCtx, vlmNr);
                }
            }
        }
        catch (InvalidNameException | ValueOutOfRangeException exc)
        {
            throw new ImplementationError(exc);
        }
        return rscGrp;
    }
}
