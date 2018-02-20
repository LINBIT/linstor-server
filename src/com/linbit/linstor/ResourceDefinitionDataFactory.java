package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.TransactionMgr;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDefinitionDataDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.stateflags.StateFlagsBits;

import javax.inject.Inject;
import java.sql.SQLException;
import java.util.UUID;

public class ResourceDefinitionDataFactory
{
    private final ResourceDefinitionDataDatabaseDriver driver;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final PropsContainerFactory propsContainerFactory;

    @Inject
    public ResourceDefinitionDataFactory(
        ResourceDefinitionDataDatabaseDriver driverRef,
        ObjectProtectionFactory objectProtectionFactoryRef,
        PropsContainerFactory propsContainerFactoryRef
    )
    {
        driver = driverRef;
        objectProtectionFactory = objectProtectionFactoryRef;
        propsContainerFactory = propsContainerFactoryRef;
    }

    public ResourceDefinitionData getInstance(
        AccessContext accCtx,
        ResourceName resName,
        TcpPortNumber port,
        ResourceDefinition.RscDfnFlags[] flags,
        String secret,
        ResourceDefinition.TransportType transType,
        TransactionMgr transMgr,
        boolean createIfNotExists,
        boolean failIfExists
    )
        throws SQLException, AccessDeniedException, LinStorDataAlreadyExistsException
    {

        ResourceDefinitionData resDfn = null;
        resDfn = driver.load(resName, false, transMgr);

        if (failIfExists && resDfn != null)
        {
            throw new LinStorDataAlreadyExistsException("The ResourceDefinition already exists");
        }

        if (resDfn == null && createIfNotExists)
        {
            resDfn = new ResourceDefinitionData(
                UUID.randomUUID(),
                objectProtectionFactory.getInstance(
                    accCtx,
                    ObjectProtection.buildPath(resName),
                    true,
                    transMgr
                ),
                resName,
                port,
                StateFlagsBits.getMask(flags),
                secret,
                transType,
                transMgr,
                driver,
                propsContainerFactory
            );
            driver.create(resDfn, transMgr);
        }
        if (resDfn != null)
        {
            resDfn.initialized();
            resDfn.setConnection(transMgr);
        }
        return resDfn;
    }

    public ResourceDefinitionData getInstanceSatellite(
        AccessContext accCtx,
        UUID uuid,
        ResourceName rscName,
        TcpPortNumber portRef,
        ResourceDefinition.RscDfnFlags[] initFlags,
        String secret,
        ResourceDefinition.TransportType transType,
        SatelliteTransactionMgr transMgr
    )
        throws ImplementationError
    {
        ResourceDefinitionData rscDfn = null;
        try
        {
            rscDfn = driver.load(rscName, false, transMgr);
            if (rscDfn == null)
            {
                rscDfn = new ResourceDefinitionData(
                    uuid,
                    objectProtectionFactory.getInstance(accCtx, "", false, transMgr),
                    rscName,
                    portRef,
                    StateFlagsBits.getMask(initFlags),
                    secret,
                    transType,
                    transMgr,
                    driver,
                    propsContainerFactory
                );
            }
            rscDfn.initialized();
            rscDfn.setConnection(transMgr);
        }
        catch (Exception exc)
        {
            throw new ImplementationError(
                "This method should only be called with a satellite db in background!",
                exc
            );
        }
        return rscDfn;
    }
}
