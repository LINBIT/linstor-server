package com.linbit.linstor;

import com.linbit.ExhaustedPoolException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.core.CtrlSecurityObjects;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDefinitionDataDatabaseDriver;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import java.sql.SQLException;
import java.util.TreeMap;
import java.util.UUID;

public class VolumeDefinitionDataControllerFactory
{
    private final VolumeDefinitionDataDatabaseDriver driver;
    private final PropsContainerFactory propsContainerFactory;
    private final DynamicNumberPool minorNrPool;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private CtrlSecurityObjects secObjs;

    @Inject
    public VolumeDefinitionDataControllerFactory(
        VolumeDefinitionDataDatabaseDriver driverRef,
        PropsContainerFactory propsContainerFactoryRef,
        @Named(NumberPoolModule.MINOR_NUMBER_POOL) DynamicNumberPool minorNrPoolRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        CtrlSecurityObjects secObjsRef
    )
    {
        driver = driverRef;
        propsContainerFactory = propsContainerFactoryRef;
        minorNrPool = minorNrPoolRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        secObjs = secObjsRef;
    }

    public VolumeDefinitionData create(
        AccessContext accCtx,
        ResourceDefinition resDfn,
        VolumeNumber volNr,
        Integer minor,
        Long volSize,
        VolumeDefinition.VlmDfnFlags[] initFlags
    )
        throws SQLException, AccessDeniedException, MdException, LinStorDataAlreadyExistsException,
        ValueOutOfRangeException, ValueInUseException, ExhaustedPoolException
    {
        resDfn.getObjProt().requireAccess(accCtx, AccessType.USE);

        VolumeDefinitionData volDfnData = driver.load(resDfn, volNr, false);

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
            chosenMinorNr = new MinorNumber(minor);
            minorNrPool.allocate(minor);
        }

        volDfnData = new VolumeDefinitionData(
            UUID.randomUUID(),
            resDfn,
            volNr,
            chosenMinorNr,
            minorNrPool,
            volSize,
            StateFlagsBits.getMask(initFlags),
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>()
        );

        driver.create(volDfnData);
        ((ResourceDefinitionData) resDfn).putVolumeDefinition(accCtx, volDfnData);

        return volDfnData;
    }

    public VolumeDefinitionData load(
        AccessContext accCtx,
        ResourceDefinition resDfn,
        VolumeNumber volNr
    )
        throws SQLException, AccessDeniedException
    {
        resDfn.getObjProt().requireAccess(accCtx, AccessType.USE);

        VolumeDefinitionData volDfnData = driver.load(resDfn, volNr, false);
        return volDfnData;
    }
}
