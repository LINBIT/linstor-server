package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.TransactionMgr;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDataDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.stateflags.StateFlagsBits;

import javax.inject.Inject;
import java.sql.SQLException;
import java.util.UUID;

public class VolumeDataFactory
{
    private final VolumeDataDatabaseDriver driver;
    private final PropsContainerFactory propsContainerFactory;

    @Inject
    public VolumeDataFactory(
        VolumeDataDatabaseDriver driverRef,
        PropsContainerFactory propsContainerFactoryRef
    )
    {
        driver = driverRef;
        propsContainerFactory = propsContainerFactoryRef;
    }

    public VolumeData getInstance(
        AccessContext accCtx,
        Resource resRef,
        VolumeDefinition volDfn,
        StorPool storPool,
        String blockDevicePathRef,
        String metaDiskPathRef,
        Volume.VlmFlags[] flags,
        TransactionMgr transMgr,
        boolean createIfNotExists,
        boolean failIfExists
    )
        throws SQLException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        resRef.getObjProt().requireAccess(accCtx, AccessType.USE);
        VolumeData volData = null;

        volData = driver.load(resRef, volDfn, false, transMgr);

        if (failIfExists && volData != null)
        {
            throw new LinStorDataAlreadyExistsException("The Volume already exists");
        }

        if (volData == null && createIfNotExists)
        {
            volData = new VolumeData(
                UUID.randomUUID(),
                accCtx,
                resRef,
                volDfn,
                storPool,
                blockDevicePathRef,
                metaDiskPathRef,
                StateFlagsBits.getMask(flags),
                transMgr,
                driver,
                propsContainerFactory
            );
            driver.create(volData, transMgr);
        }
        if (volData != null)
        {
            volData.initialized();
            volData.setConnection(transMgr);
        }
        return volData;
    }

    public VolumeData getInstanceSatellite(
        AccessContext accCtx,
        UUID vlmUuid,
        Resource rscRef,
        VolumeDefinition vlmDfn,
        StorPool storPoolRef,
        String blockDevicePathRef,
        String metaDiskPathRef,
        Volume.VlmFlags[] flags,
        SatelliteTransactionMgr transMgr
    )
    {
        VolumeData vlmData;
        try
        {
            vlmData = driver.load(rscRef, vlmDfn, false, transMgr);
            if (vlmData == null)
            {
                vlmData = new VolumeData(
                    vlmUuid,
                    accCtx,
                    rscRef,
                    vlmDfn,
                    storPoolRef,
                    blockDevicePathRef,
                    metaDiskPathRef,
                    StateFlagsBits.getMask(flags),
                    transMgr,
                    driver,
                    propsContainerFactory
                );
            }
            vlmData.initialized();
            vlmData.setConnection(transMgr);
        }
        catch (Exception exc)
        {
            throw new ImplementationError(
                "This method should only be called with a satellite db in background!",
                exc
            );
        }

        return vlmData;
    }
}
