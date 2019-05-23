package com.linbit.linstor.core.objects;

import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.core.repository.ResourceGroupRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.ResourceGroupDataDatabaseDriver;
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
import java.util.TreeMap;
import java.util.UUID;

@Singleton
public class ResourceGroupDataControllerFactory
{
    private final ResourceGroupDataDatabaseDriver driver;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final ResourceGroupRepository rscGrpRepository;

    @Inject
    public ResourceGroupDataControllerFactory(
        ResourceGroupDataDatabaseDriver driverRef,
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

    public ResourceGroupData create(
        AccessContext accCtx,
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
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        ResourceGroupData rscGrp = rscGrpRepository.get(accCtx, rscGrpName);

        if (rscGrp != null)
        {
            throw new LinStorDataAlreadyExistsException("The ResourceGroup already exists");
        }

        rscGrp = new ResourceGroupData(
            UUID.randomUUID(),
            objectProtectionFactory.getInstance(
                accCtx,
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
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider
        );

        driver.persist(rscGrp);

        return rscGrp;
    }
}
