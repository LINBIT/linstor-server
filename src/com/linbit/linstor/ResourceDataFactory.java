package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.TransactionMgr;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDataDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.stateflags.StateFlagsBits;

import javax.inject.Inject;
import java.sql.SQLException;
import java.util.UUID;

public class ResourceDataFactory
{
    private final ResourceDataDatabaseDriver dbDriver;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final PropsContainerFactory propsContainerFactory;
    private final VolumeDataFactory volumeDataFactory;

    @Inject
    public ResourceDataFactory(
        ResourceDataDatabaseDriver dbDriverRef,
        ObjectProtectionFactory objectProtectionFactoryRef,
        PropsContainerFactory propsContainerFactoryRef,
        VolumeDataFactory volumeDataFactoryRef
    )
    {
        dbDriver = dbDriverRef;
        objectProtectionFactory = objectProtectionFactoryRef;
        propsContainerFactory = propsContainerFactoryRef;
        volumeDataFactory = volumeDataFactoryRef;
    }

    public ResourceData getInstance(
        AccessContext accCtx,
        ResourceDefinition resDfn,
        Node node,
        NodeId nodeId,
        Resource.RscFlags[] initFlags,
        TransactionMgr transMgr,
        boolean createIfNotExists,
        boolean failIfExists
    )
        throws SQLException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        resDfn.getObjProt().requireAccess(accCtx, AccessType.USE);
        ResourceData resData = null;


        resData = dbDriver.load(node, resDfn.getName(), false, transMgr);

        if (failIfExists && resData != null)
        {
            throw new LinStorDataAlreadyExistsException("The Resource already exists");
        }

        if (resData == null && createIfNotExists)
        {
            resData = new ResourceData(
                UUID.randomUUID(),
                accCtx,
                objectProtectionFactory.getInstance(
                    accCtx,
                    ObjectProtection.buildPath(
                        node.getName(),
                        resDfn.getName()
                    ),
                    true,
                    transMgr
                ),
                resDfn,
                node,
                nodeId,
                StateFlagsBits.getMask(initFlags),
                transMgr,
                dbDriver,
                propsContainerFactory,
                volumeDataFactory
            );
            dbDriver.create(resData, transMgr);
        }

        if (resData != null)
        {
            resData.initialized();
            resData.setConnection(transMgr);
        }
        return resData;
    }

    public ResourceData getInstanceSatellite(
        AccessContext accCtx,
        UUID uuid,
        Node node,
        ResourceDefinition rscDfn,
        NodeId nodeId,
        Resource.RscFlags[] initFlags,
        SatelliteTransactionMgr transMgr
    )
        throws ImplementationError
    {
        ResourceData rscData;
        try
        {
            rscData = dbDriver.load(node, rscDfn.getName(), false, transMgr);
            if (rscData == null)
            {
                rscData = new ResourceData(
                    uuid,
                    accCtx,
                    objectProtectionFactory.getInstance(accCtx, "", false, transMgr),
                    rscDfn,
                    node,
                    nodeId,
                    StateFlagsBits.getMask(initFlags),
                    transMgr,
                    dbDriver,
                    propsContainerFactory,
                    volumeDataFactory
                );
            }
            rscData.initialized();
            rscData.setConnection(transMgr);
        }
        catch (Exception exc)
        {
            throw new ImplementationError(
                "This method should only be called with a satellite db in background!",
                exc
            );
        }
        return rscData;
    }
}
