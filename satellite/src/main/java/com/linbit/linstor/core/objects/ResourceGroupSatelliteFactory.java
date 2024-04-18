package com.linbit.linstor.core.objects;

import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.ResourceGroupDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeGroupDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

@Singleton
public class ResourceGroupSatelliteFactory
{
    private final AccessContext sysCtx;
    private final ResourceGroupDatabaseDriver rscGrpDriver;
    private final VolumeGroupDatabaseDriver vlmGrpDriver;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final CoreModule.ResourceGroupMap rscGrpMap;

    @Inject
    public ResourceGroupSatelliteFactory(
        @SystemContext AccessContext sysCtxRef,
        ResourceGroupDatabaseDriver rscGrpDriverRef,
        VolumeGroupDatabaseDriver vlmGrpDriverRef,
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

    public ResourceGroup getInstanceSatellite(
        UUID uuid,
        ResourceGroupName rscGrpName,
        @Nullable String description,
        @Nullable List<DeviceLayerKind> layerStackRef,
        @Nullable Integer autoPlaceReplicaCountRef,
        @Nullable List<String> autoPlaceNodeNameListRef,
        @Nullable List<String> autoPlaceStorPoolListRef,
        @Nullable List<String> autoPlaceStorPoolDisklessListRef,
        @Nullable List<String> autoPlaceDoNotPlaceWithRscListRef,
        @Nullable String autoPlaceDoNotPlaceWithRscRegexRef,
        @Nullable List<String> autoPlaceReplicasOnSameListRef,
        @Nullable List<String> autoPlaceReplicasOnDifferentListRef,
        @Nullable Map<String, Integer> autoPlaceXReplicasOnDifferentMapRef,
        @Nullable List<DeviceProviderKind> autoPlaceAllowedProviderListRef,
        @Nullable Boolean autoPlaceDisklessOnRemainingRef,
        @Nullable Short peerSlotsRef
    )
        throws DatabaseException, AccessDeniedException
    {
        ResourceGroup rscGrp = rscGrpMap.get(rscGrpName);

        if (rscGrp == null)
        {
            rscGrp = new ResourceGroup(
                uuid,
                objectProtectionFactory.getInstance(
                    sysCtx,
                    ObjectProtection.buildPath(rscGrpName),
                    true
                ),
                rscGrpName,
                description,
                copy(layerStackRef),
                autoPlaceReplicaCountRef,
                copy(autoPlaceNodeNameListRef),
                copy(autoPlaceStorPoolListRef),
                copy(autoPlaceStorPoolDisklessListRef),
                copy(autoPlaceDoNotPlaceWithRscListRef),
                autoPlaceDoNotPlaceWithRscRegexRef,
                copy(autoPlaceReplicasOnSameListRef),
                copy(autoPlaceReplicasOnDifferentListRef),
                copy(autoPlaceXReplicasOnDifferentMapRef),
                copy(autoPlaceAllowedProviderListRef),
                autoPlaceDisklessOnRemainingRef,
                new TreeMap<>(),
                new TreeMap<>(),
                peerSlotsRef,
                rscGrpDriver,
                propsContainerFactory,
                transObjFactory,
                transMgrProvider
            );
            rscGrpMap.put(rscGrpName, rscGrp);
        }
        return rscGrp;
    }

    private <T> ArrayList<T> copy(List<T> list)
    {
        return list == null ? new ArrayList<>() : new ArrayList<>(list);
    }

    private <K, V> HashMap<K, V> copy(Map<K, V> map)
    {
        return map == null ? new HashMap<>() : new HashMap<>(map);
    }
}
