package com.linbit.linstor;

import com.linbit.ExhaustedPoolException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.core.CtrlSecurityObjects;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDefinitionDataDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import javax.inject.Inject;
import javax.inject.Provider;

import java.sql.SQLException;
import java.util.TreeMap;
import java.util.UUID;

public class VolumeDefinitionDataControllerFactory
{
    private final VolumeDefinitionDataDatabaseDriver driver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final CtrlSecurityObjects secObjs;
    private final CtrlLayerStackHelper layerStackHelper;

    @Inject
    public VolumeDefinitionDataControllerFactory(
        VolumeDefinitionDataDatabaseDriver driverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        CtrlSecurityObjects secObjsRef,
        CtrlLayerStackHelper layerStackHelperRef
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
        throws SQLException, AccessDeniedException, MdException, LinStorDataAlreadyExistsException,
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

        layerStackHelper.ensureVlmDfnLayerDataExits(vlmDfnData, minor);

        driver.create(vlmDfnData);
        ((ResourceDefinitionData) rscDfn).putVolumeDefinition(accCtx, vlmDfnData);

        return vlmDfnData;
    }
}
