package com.linbit.linstor;

import com.linbit.ExhaustedPoolException;
import com.linbit.TransactionMgr;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDefinitionDataDatabaseDriver;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.stateflags.StateFlagsBits;

import javax.inject.Inject;
import javax.inject.Named;
import java.sql.SQLException;
import java.util.UUID;

public class VolumeDefinitionDataControllerFactory
{
    private final VolumeDefinitionDataDatabaseDriver driver;
    private final PropsContainerFactory propsContainerFactory;
    private final DynamicNumberPool minorNrPool;

    @Inject
    public VolumeDefinitionDataControllerFactory(
        VolumeDefinitionDataDatabaseDriver driverRef,
        PropsContainerFactory propsContainerFactoryRef,
        @Named(NumberPoolModule.MINOR_NUMBER_POOL) DynamicNumberPool minorNrPoolRef
    )
    {
        driver = driverRef;
        propsContainerFactory = propsContainerFactoryRef;
        minorNrPool = minorNrPoolRef;
    }

    public VolumeDefinitionData create(
        AccessContext accCtx,
        ResourceDefinition resDfn,
        VolumeNumber volNr,
        Integer minor,
        Long volSize,
        VolumeDefinition.VlmDfnFlags[] initFlags,
        TransactionMgr transMgr
    )
        throws SQLException, AccessDeniedException, MdException, LinStorDataAlreadyExistsException,
        ValueOutOfRangeException, ValueInUseException, ExhaustedPoolException
    {
        resDfn.getObjProt().requireAccess(accCtx, AccessType.USE);

        VolumeDefinitionData volDfnData = driver.load(resDfn, volNr, false, transMgr);

        if (volDfnData != null)
        {
            throw new LinStorDataAlreadyExistsException("The VolumeDefinition already exists");
        }

        MinorNumber chosenMinorNr;
        if (minor == null)
        {
            chosenMinorNr = new MinorNumber(minorNrPool.autoAllocate());
        }
        else
        {
            minorNrPool.allocate(minor);
            chosenMinorNr = new MinorNumber(minor);
        }

        volDfnData = new VolumeDefinitionData(
            UUID.randomUUID(),
            accCtx,
            resDfn,
            volNr,
            chosenMinorNr,
            minorNrPool,
            volSize,
            StateFlagsBits.getMask(initFlags),
            transMgr,
            driver,
            propsContainerFactory
        );
        driver.create(volDfnData, transMgr);

        volDfnData.initialized();
        volDfnData.setConnection(transMgr);
        return volDfnData;
    }

    public VolumeDefinitionData load(
        AccessContext accCtx,
        ResourceDefinition resDfn,
        VolumeNumber volNr,
        TransactionMgr transMgr
    )
        throws SQLException, AccessDeniedException
    {
        resDfn.getObjProt().requireAccess(accCtx, AccessType.USE);

        VolumeDefinitionData volDfnData = driver.load(resDfn, volNr, false, transMgr);

        if (volDfnData != null)
        {
            volDfnData.initialized();
            volDfnData.setConnection(transMgr);
        }
        return volDfnData;
    }
}
