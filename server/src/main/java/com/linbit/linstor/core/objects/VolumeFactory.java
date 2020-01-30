package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.TreeMap;
import java.util.UUID;

public class VolumeFactory
{
    private final VolumeDatabaseDriver driver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public VolumeFactory(
        VolumeDatabaseDriver driverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        driver = driverRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public Volume create(
        AccessContext accCtx,
        Resource rsc,
        VolumeDefinition vlmDfn,
        StorPool storPool,
        String blockDevicePathRef,
        String metaDiskPathRef,
        Volume.Flags[] flags
    )
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        rsc.getObjProt().requireAccess(accCtx, AccessType.USE);
        Volume volData = null;

        volData = rsc.getVolume(vlmDfn.getVolumeNumber());

        if (volData != null)
        {
            throw new LinStorDataAlreadyExistsException("The Volume already exists");
        }

        volData = new Volume(
            UUID.randomUUID(),
            rsc,
            vlmDfn,
            StateFlagsBits.getMask(flags),
            driver,
            new TreeMap<>(),
            propsContainerFactory,
            transObjFactory,
            transMgrProvider
        );
        driver.create(volData);
        rsc.putVolume(accCtx, volData);
        vlmDfn.putVolume(accCtx, volData);

        return volData;
    }

    public Volume getInstanceSatellite(
        AccessContext accCtx,
        UUID vlmUuid,
        Resource rsc,
        VolumeDefinition vlmDfn,
        Volume.Flags[] flags
    )
    {
        Volume vlmData;
        try
        {
            vlmData = rsc.getVolume(vlmDfn.getVolumeNumber());
            if (vlmData == null)
            {
                vlmData = new Volume(
                    vlmUuid,
                    rsc,
                    vlmDfn,
                    StateFlagsBits.getMask(flags),
                    driver,
                    new TreeMap<>(),
                    propsContainerFactory,
                    transObjFactory,
                    transMgrProvider
                );
                rsc.putVolume(accCtx, vlmData);
                vlmDfn.putVolume(accCtx, vlmData);

                vlmData.setAllocatedSize(accCtx, vlmDfn.getVolumeSize(accCtx));
                // usable size depends on deviceLayer
            }
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
