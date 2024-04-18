package com.linbit.linstor.core.objects;

import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.core.repository.ResourceGroupRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.ResourceGroupDatabaseDriver;
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
public class ResourceGroupControllerFactory
{
    private final ResourceGroupDatabaseDriver driver;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final ResourceGroupRepository rscGrpRepository;

    @Inject
    public ResourceGroupControllerFactory(
        ResourceGroupDatabaseDriver driverRef,
        ObjectProtectionFactory objectProtectionFactoryRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        ResourceGroupRepository resourceGroupRepositoryRef
    )
    {
        driver = driverRef;
        objectProtectionFactory = objectProtectionFactoryRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        rscGrpRepository = resourceGroupRepositoryRef;
    }

    public ResourceGroup create(
        AccessContext accCtx,
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
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        ResourceGroup rscGrp = rscGrpRepository.get(accCtx, rscGrpName);

        if (rscGrp != null)
        {
            throw new LinStorDataAlreadyExistsException("The ResourceGroup already exists");
        }

        rscGrp = new ResourceGroup(
            UUID.randomUUID(),
            objectProtectionFactory.getInstance(
                accCtx,
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
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider
        );

        driver.create(rscGrp);

        return rscGrp;
    }

    private <T> ArrayList<T> copy(List<T> source)
    {
        ArrayList<T> copiedList;
        if (source == null)
        {
            copiedList = new ArrayList<>();
        }
        else
        {
            copiedList = new ArrayList<>(source);
        }
        return copiedList;
    }

    private <K, V> HashMap<K, V> copy(Map<K, V> source)
    {
        HashMap<K, V> copiedMap;
        if (source == null)
        {
            copiedMap = new HashMap<>();
        }
        else
        {
            copiedMap = new HashMap<>(source);
        }
        return copiedMap;
    }
}
