package com.linbit.linstor.core.objects;

import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.LayerLuksRscCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.storage.data.adapter.luks.LuksRscData;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgrSQL;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Singleton
public class LayerLuksRscDbDriver
    extends AbsLayerRscDataDbDriver<RscDfnLayerObject, VlmDfnLayerObject, LuksRscData<?>, LuksVlmData<?>>
    implements LayerLuksRscCtrlDatabaseDriver
{
    private final LayerLuksVlmDbDriver layerLuksVlmDbDriver;

    @Inject
    public LayerLuksRscDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        ObjectProtectionFactory objProtFactoryRef,
        LayerResourceIdDatabaseDriver rscLayerIdDriverRef,
        LayerLuksVlmDbDriver layerLuksVlmDbDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgrSQL> transMgrProviderRef
    )
    {
        super(
            dbCtxRef,
            errorReporterRef,
            null,
            null,
            dbEngineRef,
            objProtFactoryRef,
            rscLayerIdDriverRef,
            null,
            null,
            layerLuksVlmDbDriverRef,
            transObjFactoryRef,
            transMgrProviderRef
        );
        layerLuksVlmDbDriver = layerLuksVlmDbDriverRef;
    }

    @Override
    public DeviceLayerKind getDeviceLayerKind()
    {
        return DeviceLayerKind.LUKS;
    }

    @Override
    protected <RSC extends AbsResource<RSC>> RscDataLoadOutput<LuksRscData<?>, LuksVlmData<?>> loadImpl(
        RawParameters rawRef,
        ParentObjects parentRef,
        AbsRscLayerObject<?> currentDummyLoadingRLORef,
        Pair<NodeName, SuffixedResourceName> nodeNameSuffixedRscNamePairRef,
        @Nullable AbsRscLayerObject<?> loadedParentRscDataRef,
        RSC absRscRef
    )
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException, MdException
    {
        AbsRscLayerObject<?> absRscLayerObject = rawRef.get(NULL_TABLE_LAYER_RSC_DATA_COLUMN);
        Map<VolumeNumber, LuksVlmData<?>> vlmMap = new HashMap<>();
        Set<AbsRscLayerObject<?>> childrenSet = new HashSet<>();

        LuksRscData<?> rscData = genericCreate(
            absRscLayerObject,
            loadedParentRscDataRef,
            absRscRef,
            childrenSet,
            vlmMap
        );
        return new RscDataLoadOutput<>(rscData, childrenSet, vlmMap);
    }

    @SuppressWarnings("unchecked")
    private <RSC extends AbsResource<RSC>> LuksRscData<RSC> genericCreate(
        AbsRscLayerObject<?> absRscLayerObjectRef,
        @Nullable AbsRscLayerObject<?> loadedParentRscDataRef,
        RSC absRscRef,
        Set<AbsRscLayerObject<?>> childrenSetRef,
        Map<VolumeNumber, LuksVlmData<?>> vlmMapRef
    )
    {
        Object typelessVlmMap = vlmMapRef;
        Map<VolumeNumber, LuksVlmData<RSC>> typedVlmMap = (Map<VolumeNumber, LuksVlmData<RSC>>) typelessVlmMap;

        Object typelessChildrenSet = childrenSetRef;
        Set<AbsRscLayerObject<RSC>> typedChildrenSet = (Set<AbsRscLayerObject<RSC>>) typelessChildrenSet;

        return new LuksRscData<>(
            absRscLayerObjectRef.getRscLayerId(),
            absRscRef,
            absRscLayerObjectRef.getResourceNameSuffix(),
            (AbsRscLayerObject<RSC>) loadedParentRscDataRef,
            typedChildrenSet,
            typedVlmMap,
            this,
            layerLuksVlmDbDriver,
            transObjFactory,
            transMgrProvider
        );
    }
}
