package com.linbit.linstor.core.objects;

import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDatabaseDriver;
import com.linbit.linstor.layer.CtrlLayerDataHelper;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.UUID;

public class VolumeControllerFactory
{
    private final VolumeDatabaseDriver driver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final CtrlLayerDataHelper layerStackHelper;

    @Inject
    public VolumeControllerFactory(
        VolumeDatabaseDriver driverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        CtrlLayerDataHelper layerStackHelperRef
    )
    {
        driver = driverRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        layerStackHelper = layerStackHelperRef;
    }

    public Volume create(
        AccessContext accCtx,
        Resource rsc,
        VolumeDefinition vlmDfn,
        Volume.Flags[] flags,
        Map<String, StorPool> storPoolMapRef
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
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>()
        );

        driver.create(volData);
        rsc.putVolume(accCtx, volData);
        vlmDfn.putVolume(accCtx, volData);

        LayerPayload payload = new LayerPayload();
        int vlmNr = vlmDfn.getVolumeNumber().value;
        for (Entry<String, StorPool> entry : storPoolMapRef.entrySet())
        {
            payload.putStorageVlmPayload(entry.getKey(), vlmNr, entry.getValue().getName().displayValue);
        }
        layerStackHelper.ensureStackDataExists(rsc, null, payload);

        return volData;
    }
}
