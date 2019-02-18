package com.linbit.linstor;

import com.linbit.ExhaustedPoolException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.ResourceDefinition.RscDfnFlags;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDefinitionDataDatabaseDriver;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.TreeMap;
import java.util.UUID;

@Singleton
public class ResourceDefinitionDataControllerFactory
{
    private final ResourceDefinitionDataDatabaseDriver driver;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final PropsContainerFactory propsContainerFactory;
    private final DynamicNumberPool tcpPortPool;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final ResourceDefinitionRepository resourceDefinitionRepository;

    @Inject
    public ResourceDefinitionDataControllerFactory(
        ResourceDefinitionDataDatabaseDriver driverRef,
        ObjectProtectionFactory objectProtectionFactoryRef,
        PropsContainerFactory propsContainerFactoryRef,
        @Named(NumberPoolModule.TCP_PORT_POOL) DynamicNumberPool tcpPortPoolRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        ResourceDefinitionRepository resourceDefinitionRepositoryRef
    )
    {
        driver = driverRef;
        objectProtectionFactory = objectProtectionFactoryRef;
        propsContainerFactory = propsContainerFactoryRef;
        tcpPortPool = tcpPortPoolRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        resourceDefinitionRepository = resourceDefinitionRepositoryRef;
    }

    public ResourceDefinitionData create(
        AccessContext accCtx,
        ResourceName rscName,
        Integer port,
        RscDfnFlags[] flags,
        String secret,
        TransportType transType
    )
        throws SQLException, AccessDeniedException, LinStorDataAlreadyExistsException,
        ValueOutOfRangeException, ValueInUseException, ExhaustedPoolException
    {
        ResourceDefinitionData rscDfn = resourceDefinitionRepository.get(accCtx, rscName);

        if (rscDfn != null)
        {
            throw new LinStorDataAlreadyExistsException("The ResourceDefinition already exists");
        }

        TcpPortNumber chosenTcpPort;
        if (port == null)
        {
            chosenTcpPort = new TcpPortNumber(tcpPortPool.autoAllocate());
        }
        else
        {
            chosenTcpPort = new TcpPortNumber(port);
            tcpPortPool.allocate(port);
        }

        rscDfn = new ResourceDefinitionData(
            UUID.randomUUID(),
            objectProtectionFactory.getInstance(
                accCtx,
                ObjectProtection.buildPath(rscName),
                true
            ),
            rscName,
            chosenTcpPort,
            tcpPortPool,
            StateFlagsBits.getMask(flags),
            secret,
            transType,
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

        return rscDfn;
    }
}
