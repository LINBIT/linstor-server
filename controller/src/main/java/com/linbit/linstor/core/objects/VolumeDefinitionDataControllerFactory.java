package com.linbit.linstor.core.objects;

import com.linbit.ExhaustedPoolException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.core.CtrlSecurityObjects;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceDefinitionData;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.objects.VolumeDefinitionData;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDefinitionDataDatabaseDriver;
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
import java.util.TreeMap;
import java.util.UUID;

public class VolumeDefinitionDataControllerFactory
{
    private final VolumeDefinitionDataDatabaseDriver driver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final CtrlSecurityObjects secObjs;
    private final CtrlLayerDataHelper layerStackHelper;

    @Inject
    public VolumeDefinitionDataControllerFactory(
        VolumeDefinitionDataDatabaseDriver driverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        CtrlSecurityObjects secObjsRef,
        CtrlLayerDataHelper layerStackHelperRef
    )
    {
        driver = driverRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        secObjs = secObjsRef;
        layerStackHelper = layerStackHelperRef;
    }

    public VolumeDefinitionData create(
        AccessContext accCtx,
        ResourceDefinition rscDfn,
        VolumeNumber vlmNr,
        Integer minor,
        Long vlmSize,
        VolumeDefinition.VlmDfnFlags[] initFlags
    )
        throws DatabaseException, AccessDeniedException, MdException, LinStorDataAlreadyExistsException,
        ValueOutOfRangeException, ValueInUseException, ExhaustedPoolException
    {

        rscDfn.getObjProt().requireAccess(accCtx, AccessType.USE);

        VolumeDefinitionData vlmDfnData = (VolumeDefinitionData) rscDfn.getVolumeDfn(accCtx, vlmNr);

        if (vlmDfnData != null)
        {
            throw new LinStorDataAlreadyExistsException("The VolumeDefinition already exists");
        }

        vlmDfnData = new VolumeDefinitionData(
            UUID.randomUUID(),
            rscDfn,
            vlmNr,
            vlmSize,
            StateFlagsBits.getMask(initFlags),
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>(),
            new TreeMap<>()
        );

        driver.create(vlmDfnData);
        ((ResourceDefinitionData) rscDfn).putVolumeDefinition(accCtx, vlmDfnData);

        // TODO: might be a good idea to create this object earlier
        LayerPayload payload = new LayerPayload();
        payload.getDrbdVlmDfn().setMinorNr(minor);
        layerStackHelper.ensureVlmDfnLayerDataExits(vlmDfnData, "", payload);

        return vlmDfnData;
    }
}
