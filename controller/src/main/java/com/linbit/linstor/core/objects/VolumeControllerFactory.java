package com.linbit.linstor.core.objects;

import com.linbit.drbd.md.MaxSizeException;
import com.linbit.drbd.md.MinSizeException;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.core.objects.utils.MixedStorPoolHelper;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDatabaseDriver;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.layer.resource.CtrlRscLayerDataFactory;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

public class VolumeControllerFactory
{
    private final VolumeDatabaseDriver driver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final CtrlRscLayerDataFactory layerStackHelper;
    private final MixedStorPoolHelper mixedStorPoolHelper;

    @Inject
    public VolumeControllerFactory(
        VolumeDatabaseDriver driverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        CtrlRscLayerDataFactory layerStackHelperRef,
        MixedStorPoolHelper mixedStorPoolHelperRef
    )
    {
        driver = driverRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        layerStackHelper = layerStackHelperRef;
        mixedStorPoolHelper = mixedStorPoolHelperRef;
    }

    public <RSC extends AbsResource<RSC>> Volume create(
        AccessContext accCtx,
        Resource rsc,
        VolumeDefinition vlmDfn,
        @Nullable Volume.Flags[] flags,
        LayerPayload payload,
        @Nullable AbsRscLayerObject<RSC> absLayerData,
        Map<String, String> storpoolRenameMap,
        @Nullable ApiCallRc apiCallRc
    )
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException, MinSizeException,
        MaxSizeException, StorageException
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

        if (absLayerData == null)
        {
            layerStackHelper.ensureStackDataExists(rsc, null, payload);
        }
        else
        {
            // ignore payload if we have snapLayerData
            layerStackHelper.copyLayerData(absLayerData, rsc, storpoolRenameMap, apiCallRc);
        }

        mixedStorPoolHelper.handleMixedStoragePools(volData);

        vlmDfn.recheckVolumeSize(accCtx);

        return volData;
    }
}
