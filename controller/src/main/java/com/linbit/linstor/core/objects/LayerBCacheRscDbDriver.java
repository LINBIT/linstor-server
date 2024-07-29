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
import com.linbit.linstor.dbdrivers.interfaces.LayerBCacheRscCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.storage.data.adapter.bcache.BCacheRscData;
import com.linbit.linstor.storage.data.adapter.bcache.BCacheVlmData;
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
public class LayerBCacheRscDbDriver
    extends AbsLayerRscDataDbDriver<RscDfnLayerObject, VlmDfnLayerObject, BCacheRscData<?>, BCacheVlmData<?>>
    implements LayerBCacheRscCtrlDatabaseDriver
{
    private final LayerBCacheVlmDbDriver layerBCacheVlmDbDriver;

    @Inject
    public LayerBCacheRscDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        ObjectProtectionFactory objProtFactoryRef,
        LayerResourceIdDatabaseDriver rscLayerIdDriverRef,
        LayerBCacheVlmDbDriver layerBCacheVlmDbDriverRef,
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
            layerBCacheVlmDbDriverRef,
            transObjFactoryRef,
            transMgrProviderRef
        );
        layerBCacheVlmDbDriver = layerBCacheVlmDbDriverRef;
    }

    @Override
    public DeviceLayerKind getDeviceLayerKind()
    {
        return DeviceLayerKind.BCACHE;
    }

    @Override
    protected <RSC extends AbsResource<RSC>> RscDataLoadOutput<BCacheRscData<?>, BCacheVlmData<?>> loadImpl(
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
        Map<VolumeNumber, BCacheVlmData<?>> vlmMap = new HashMap<>();
        Set<AbsRscLayerObject<?>> childrenSet = new HashSet<>();

        BCacheRscData<?> rscData = genericCreate(
            absRscLayerObject,
            loadedParentRscDataRef,
            absRscRef,
            childrenSet,
            vlmMap
        );
        return new RscDataLoadOutput<>(rscData, childrenSet, vlmMap);
    }

    @SuppressWarnings("unchecked")
    private <RSC extends AbsResource<RSC>> BCacheRscData<RSC> genericCreate(
        AbsRscLayerObject<?> absRscLayerObjectRef,
        @Nullable AbsRscLayerObject<?> loadedParentRscDataRef,
        RSC absRscRef,
        Set<AbsRscLayerObject<?>> childrenSetRef,
        Map<VolumeNumber, BCacheVlmData<?>> vlmMapRef
    )
    {
        Object typelessVlmMap = vlmMapRef;
        Object typelessChildrenSet = childrenSetRef;
        return new BCacheRscData<>(
            absRscLayerObjectRef.getRscLayerId(),
            absRscRef,
            (AbsRscLayerObject<RSC>) loadedParentRscDataRef,
            (Set<AbsRscLayerObject<RSC>>) typelessChildrenSet,
            absRscLayerObjectRef.getResourceNameSuffix(),
            this,
            layerBCacheVlmDbDriver,
            (Map<VolumeNumber, BCacheVlmData<RSC>>) typelessVlmMap,
            transObjFactory,
            transMgrProvider
        );
    }
}
