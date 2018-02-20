package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.TransactionMgr;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDefinitionDataDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.stateflags.StateFlagsBits;

import javax.inject.Inject;
import java.sql.SQLException;
import java.util.UUID;

public class VolumeDefinitionDataFactory
{
    private final VolumeDefinitionDataDatabaseDriver driver;
    private final PropsContainerFactory propsContainerFactory;

    @Inject
    public VolumeDefinitionDataFactory(
        VolumeDefinitionDataDatabaseDriver driverRef,
        PropsContainerFactory propsContainerFactoryRef
    )
    {
        driver = driverRef;
        propsContainerFactory = propsContainerFactoryRef;
    }

    public VolumeDefinitionData getInstance(
        AccessContext accCtx,
        ResourceDefinition resDfn,
        VolumeNumber volNr,
        MinorNumber minor,
        Long volSize,
        VolumeDefinition.VlmDfnFlags[] initFlags,
        TransactionMgr transMgr,
        boolean createIfNotExists,
        boolean failIfExists
    )
        throws SQLException, AccessDeniedException, MdException, LinStorDataAlreadyExistsException
    {
        resDfn.getObjProt().requireAccess(accCtx, AccessType.USE);
        VolumeDefinitionData volDfnData = null;


        volDfnData = driver.load(resDfn, volNr, false, transMgr);

        if (failIfExists && volDfnData != null)
        {
            throw new LinStorDataAlreadyExistsException("The VolumeDefinition already exists");
        }

        if (volDfnData == null && createIfNotExists)
        {
            volDfnData = new VolumeDefinitionData(
                UUID.randomUUID(),
                accCtx,
                resDfn,
                volNr,
                minor,
                volSize,
                StateFlagsBits.getMask(initFlags),
                transMgr,
                driver,
                propsContainerFactory
            );
            driver.create(volDfnData, transMgr);
        }

        if (volDfnData != null)
        {
            volDfnData.initialized();
            volDfnData.setConnection(transMgr);
        }
        return volDfnData;
    }

    public VolumeDefinitionData getInstanceSatellite(
        AccessContext accCtx,
        UUID vlmDfnUuid,
        ResourceDefinition rscDfn,
        VolumeNumber vlmNr,
        long vlmSize,
        MinorNumber minorNumber,
        VolumeDefinition.VlmDfnFlags[] flags,
        SatelliteTransactionMgr transMgr
    )
        throws ImplementationError
    {
        VolumeDefinitionData vlmDfnData;
        try
        {
            vlmDfnData = driver.load(rscDfn, vlmNr, false, transMgr);
            if (vlmDfnData == null)
            {
                vlmDfnData = new VolumeDefinitionData(
                    vlmDfnUuid,
                    accCtx,
                    rscDfn,
                    vlmNr,
                    minorNumber,
                    vlmSize,
                    StateFlagsBits.getMask(flags),
                    transMgr,
                    driver,
                    propsContainerFactory
                );
            }
            vlmDfnData.initialized();
            vlmDfnData.setConnection(transMgr);
        }
        catch (Exception exc)
        {
            throw new ImplementationError(
                "This method should only be called with a satellite db in background!",
                exc
            );
        }
        return vlmDfnData;
    }
}
