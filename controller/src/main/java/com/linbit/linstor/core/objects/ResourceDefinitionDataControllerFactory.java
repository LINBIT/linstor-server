package com.linbit.linstor.core.objects;

import com.linbit.ExhaustedPoolException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.core.objects.ResourceDefinitionData;
import com.linbit.linstor.core.objects.ResourceDefinition.RscDfnFlags;
import com.linbit.linstor.core.objects.ResourceDefinition.TransportType;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDefinitionDataDatabaseDriver;
import com.linbit.linstor.layer.CtrlLayerDataHelper;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.layer.LayerPayload.DrbdRscDfnPayload;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;

@Singleton
public class ResourceDefinitionDataControllerFactory
{
    private final ResourceDefinitionDataDatabaseDriver driver;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final ResourceDefinitionRepository resourceDefinitionRepository;
    private final CtrlLayerDataHelper layerStackHelper;

    @Inject
    public ResourceDefinitionDataControllerFactory(
        ResourceDefinitionDataDatabaseDriver driverRef,
        ObjectProtectionFactory objectProtectionFactoryRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        ResourceDefinitionRepository resourceDefinitionRepositoryRef,
        CtrlLayerDataHelper layerStackHelperRef
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

    public ResourceDefinitionData create(
        AccessContext accCtx,
        ResourceName rscName,
        byte[] extName,
        Integer port,
        RscDfnFlags[] flags,
        String secret,
        TransportType transType,
        List<DeviceLayerKind> layerStack,
        Short peerSlotsRef
    )
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException,
        ValueOutOfRangeException, ValueInUseException, ExhaustedPoolException
    {
        ResourceDefinitionData rscDfn = resourceDefinitionRepository.get(accCtx, rscName);

        if (rscDfn != null)
        {
            throw new LinStorDataAlreadyExistsException("The ResourceDefinition already exists");
        }

        rscDfn = new ResourceDefinitionData(
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
            new TreeMap<>()
        );

        driver.create(rscDfn);

        // TODO: might be a good idea to create this object earlier
        LayerPayload payload = new LayerPayload();
        DrbdRscDfnPayload drbdRscDfn = payload.getDrbdRscDfn();
        drbdRscDfn.setPeerSlotsNewResource(peerSlotsRef);
        drbdRscDfn.setTcpPort(port);
        drbdRscDfn.setSharedSecret(secret);
        drbdRscDfn.setTransportType(transType);
        layerStackHelper.ensureRequiredRscDfnLayerDataExits(rscDfn, "", payload);

        return rscDfn;
    }
}
