package com.linbit.linstor;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.ResourceDefinitionMap;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDefinitionDataDatabaseDriver;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ControllerSecurityModule;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import java.sql.SQLException;
import java.util.TreeMap;
import java.util.UUID;

public class ResourceDefinitionDataControllerFactory
{
    private final ResourceDefinitionDataDatabaseDriver driver;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final PropsContainerFactory propsContainerFactory;
    private final DynamicNumberPool tcpPortPool;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final ResourceDefinitionMap rscDfnMap;
    private final ObjectProtection rscDfnMapProt;

    @Inject
    public ResourceDefinitionDataControllerFactory(
        ResourceDefinitionDataDatabaseDriver driverRef,
        ObjectProtectionFactory objectProtectionFactoryRef,
        PropsContainerFactory propsContainerFactoryRef,
        @Named(NumberPoolModule.TCP_PORT_POOL) DynamicNumberPool tcpPortPoolRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        @Named(ControllerSecurityModule.RSC_DFN_MAP_PROT) ObjectProtection rscDfnMapProtRef
    )
    {
        driver = driverRef;
        objectProtectionFactory = objectProtectionFactoryRef;
        propsContainerFactory = propsContainerFactoryRef;
        tcpPortPool = tcpPortPoolRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        rscDfnMap = rscDfnMapRef;
        rscDfnMapProt = rscDfnMapProtRef;
    }

    public ResourceDefinitionData getInstance(AccessContext accCtx, ResourceName rscName)
        throws AccessDeniedException
    {
        rscDfnMapProt.requireAccess(accCtx, AccessType.VIEW);
        return (ResourceDefinitionData) rscDfnMap.get(rscName);
    }

    public ResourceDefinitionData create(
        AccessContext accCtx,
        ResourceName rscName,
        Integer port,
        ResourceDefinition.RscDfnFlags[] flags,
        String secret,
        ResourceDefinition.TransportType transType
    )
        throws SQLException, AccessDeniedException, LinStorDataAlreadyExistsException,
        ValueOutOfRangeException, ValueInUseException, ExhaustedPoolException
    {
        ResourceDefinitionData rscDfn = getInstance(accCtx, rscName);

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
            new TreeMap<>()
        );
        driver.create(rscDfn);

        return rscDfn;
    }
}
