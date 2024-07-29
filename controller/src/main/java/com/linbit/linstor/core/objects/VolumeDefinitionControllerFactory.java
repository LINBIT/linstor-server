package com.linbit.linstor.core.objects;

import com.linbit.ExhaustedPoolException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.CtrlSecurityObjects;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDefinitionDatabaseDriver;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.layer.resource.CtrlRscLayerDataFactory;
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

public class VolumeDefinitionControllerFactory
{
    private final VolumeDefinitionDatabaseDriver driver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final CtrlSecurityObjects secObjs;
    private final CtrlRscLayerDataFactory layerStackHelper;

    @Inject
    public VolumeDefinitionControllerFactory(
        VolumeDefinitionDatabaseDriver driverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        CtrlSecurityObjects secObjsRef,
        CtrlRscLayerDataFactory layerStackHelperRef
    )
    {
        driver = driverRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        secObjs = secObjsRef;
        layerStackHelper = layerStackHelperRef;
    }

    public VolumeDefinition create(
        AccessContext accCtx,
        ResourceDefinition rscDfn,
        VolumeNumber vlmNr,
        @Nullable Integer minor,
        Long vlmSize,
        @Nullable VolumeDefinition.Flags[] initFlags
    )
        throws DatabaseException, AccessDeniedException, MdException, LinStorDataAlreadyExistsException,
        ValueOutOfRangeException, ValueInUseException, ExhaustedPoolException, LinStorException
    {

        rscDfn.getObjProt().requireAccess(accCtx, AccessType.USE);

        VolumeDefinition vlmDfnData = rscDfn.getVolumeDfn(accCtx, vlmNr);

        if (vlmDfnData != null)
        {
            throw new LinStorDataAlreadyExistsException("The VolumeDefinition already exists");
        }

        vlmDfnData = new VolumeDefinition(
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
        rscDfn.putVolumeDefinition(accCtx, vlmDfnData);

        // TODO: might be a good idea to create this object earlier
        LayerPayload payload = new LayerPayload();
        payload.getDrbdVlmDfn().minorNr = minor;
        layerStackHelper.ensureVlmDfnLayerDataExits(vlmDfnData, "", payload);

        return vlmDfnData;
    }
}
