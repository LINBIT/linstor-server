package com.linbit.linstor.core.objects;

import com.linbit.ExhaustedPoolException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDefinitionDatabaseDriver;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.layer.resource.CtrlRscLayerDataFactory;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.List;
import java.util.TreeMap;
import java.util.UUID;

@Singleton
public class ResourceDefinitionControllerFactory
{
    private final ResourceDefinitionDatabaseDriver driver;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final ResourceDefinitionRepository resourceDefinitionRepository;
    private final CtrlRscLayerDataFactory layerStackHelper;

    @Inject
    public ResourceDefinitionControllerFactory(
        ResourceDefinitionDatabaseDriver driverRef,
        ObjectProtectionFactory objectProtectionFactoryRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        ResourceDefinitionRepository resourceDefinitionRepositoryRef,
        CtrlRscLayerDataFactory layerStackHelperRef
    )
    {
        driver = driverRef;
        objectProtectionFactory = objectProtectionFactoryRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        resourceDefinitionRepository = resourceDefinitionRepositoryRef;
        layerStackHelper = layerStackHelperRef;
    }

    public ResourceDefinition create(
        AccessContext accCtx,
        ResourceName rscName,
        @Nullable byte[] extName,
        @Nullable ResourceDefinition.Flags[] flags,
        @Nullable List<DeviceLayerKind> layerStack,
        LayerPayload payload,
        ResourceGroup rscGroup
    )
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException,
        ValueOutOfRangeException, ValueInUseException, ExhaustedPoolException, LinStorException
    {
        ResourceDefinition rscDfn = resourceDefinitionRepository.get(accCtx, rscName);

        if (rscDfn != null)
        {
            throw new LinStorDataAlreadyExistsException("The ResourceDefinition already exists");
        }

        rscDfn = new ResourceDefinition(
            UUID.randomUUID(),
            objectProtectionFactory.getInstance(
                accCtx,
                ObjectProtection.buildPath(rscName),
                true
            ),
            rscName,
            extName,
            StateFlagsBits.getMask(flags),
            layerStack,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>(),
            new TreeMap<>(),
            new TreeMap<>(),
            new TreeMap<>(),
            rscGroup
        );

        rscGroup.addResourceDefinition(accCtx, rscDfn);

        driver.create(rscDfn);

        layerStackHelper.ensureRequiredRscDfnLayerDataExits(rscDfn, "", payload);

        return rscDfn;
    }
}
