package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.drbd.md.MaxSizeException;
import com.linbit.drbd.md.MinSizeException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDatabaseDriver;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.layer.resource.CtrlRscLayerDataFactory;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.utils.layer.LayerVlmUtils;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

public class VolumeControllerFactory
{
    private final VolumeDatabaseDriver driver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final CtrlRscLayerDataFactory layerStackHelper;

    @Inject
    public VolumeControllerFactory(
        VolumeDatabaseDriver driverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        CtrlRscLayerDataFactory layerStackHelperRef
    )
    {
        driver = driverRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        layerStackHelper = layerStackHelperRef;
    }

    public <RSC extends AbsResource<RSC>> Volume create(
        AccessContext accCtx,
        Resource rsc,
        VolumeDefinition vlmDfn,
        Volume.Flags[] flags,
        LayerPayload payload,
        @Nullable AbsRscLayerObject<RSC> absLayerData
    )
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException, MinSizeException,
        MaxSizeException
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
            layerStackHelper.copyLayerData(absLayerData, rsc);
        }

        if (hasMixedStoragePools(accCtx, volData))
        {
            try
            {
                volData.getVolumeDefinition()
                    .getResourceDefinition()
                    .getProps(accCtx)
                    .setProp(
                        InternalApiConsts.KEY_FORCE_INITIAL_SYNC_PERMA,
                        ApiConsts.VAL_TRUE,
                        ApiConsts.NAMESPC_DRBD_OPTIONS
                    );
            }
            catch (InvalidKeyException | InvalidValueException exc)
            {
                throw new ImplementationError(exc);
            }
        }

        vlmDfn.recheckVolumeSize(accCtx);

        return volData;
    }

    private boolean hasMixedStoragePools(AccessContext accCtx, Volume volDataRef) throws AccessDeniedException
    {
        boolean usesThick = false;
        boolean usesThin = false;
        Iterator<Resource> rscIt = volDataRef.getVolumeDefinition().getResourceDefinition().iterateResource(accCtx);
        while (rscIt.hasNext())
        {
            Resource rsc = rscIt.next();
            Set<StorPool> storPools = LayerVlmUtils.getStorPools(rsc, accCtx, false);
            for (StorPool sp : storPools)
            {
                if (sp.getDeviceProviderKind().usesThinProvisioning())
                {
                    usesThin = true;
                }
                else
                {
                    usesThick = true;
                }
            }
        }

        return usesThick && usesThin;
    }
}
