package com.linbit.linstor;

import com.linbit.ExhaustedPoolException;
import com.linbit.TransactionMgr;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDefinitionDataDatabaseDriver;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.stateflags.StateFlagsBits;

import javax.inject.Inject;
import javax.inject.Named;
import java.sql.SQLException;
import java.util.UUID;

public class ResourceDefinitionDataControllerFactory
{
    private final ResourceDefinitionDataDatabaseDriver driver;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final PropsContainerFactory propsContainerFactory;
    private final DynamicNumberPool tcpPortPool;

    @Inject
    public ResourceDefinitionDataControllerFactory(
        ResourceDefinitionDataDatabaseDriver driverRef,
        ObjectProtectionFactory objectProtectionFactoryRef,
        PropsContainerFactory propsContainerFactoryRef,
        @Named(NumberPoolModule.TCP_PORT_POOL) DynamicNumberPool tcpPortPoolRef
    )
    {
        driver = driverRef;
        objectProtectionFactory = objectProtectionFactoryRef;
        propsContainerFactory = propsContainerFactoryRef;
        tcpPortPool = tcpPortPoolRef;
    }

    public ResourceDefinitionData create(
        AccessContext accCtx,
        ResourceName resName,
        Integer port,
        ResourceDefinition.RscDfnFlags[] flags,
        String secret,
        ResourceDefinition.TransportType transType,
        TransactionMgr transMgr
    )
        throws SQLException, AccessDeniedException, LinStorDataAlreadyExistsException,
        ValueOutOfRangeException, ValueInUseException, ExhaustedPoolException
    {

        ResourceDefinitionData resDfn = null;
        resDfn = driver.load(resName, false, transMgr);

        if (resDfn != null)
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
            tcpPortPool.allocate(port);
            chosenTcpPort = new TcpPortNumber(port);
        }

        resDfn = new ResourceDefinitionData(
            UUID.randomUUID(),
            objectProtectionFactory.getInstance(
                accCtx,
                ObjectProtection.buildPath(resName),
                true,
                transMgr
            ),
            resName,
            chosenTcpPort,
            tcpPortPool,
            StateFlagsBits.getMask(flags),
            secret,
            transType,
            transMgr,
            driver,
            propsContainerFactory
        );
        driver.create(resDfn, transMgr);

        resDfn.initialized();
        resDfn.setConnection(transMgr);
        return resDfn;
    }

    public ResourceDefinitionData load(
        ResourceName resName,
        TransactionMgr transMgr
    )
        throws SQLException
    {

        ResourceDefinitionData resDfn = driver.load(resName, false, transMgr);

        if (resDfn != null)
        {
            resDfn.initialized();
            resDfn.setConnection(transMgr);
        }
        return resDfn;
    }
}
