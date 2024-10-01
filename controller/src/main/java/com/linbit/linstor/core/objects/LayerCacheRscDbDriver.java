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
import com.linbit.linstor.dbdrivers.interfaces.LayerCacheRscCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.data.adapter.cache.CacheRscData;
import com.linbit.linstor.storage.data.adapter.cache.CacheVlmData;
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
public class LayerCacheRscDbDriver
    extends AbsLayerRscDataDbDriver<RscDfnLayerObject, VlmDfnLayerObject, CacheRscData<?>, CacheVlmData<?>>
    implements LayerCacheRscCtrlDatabaseDriver
{
    private final LayerCacheVlmDbDriver layerCacheVlmDbDriver;

    @Inject
    public LayerCacheRscDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        LayerResourceIdDatabaseDriver rscLayerIdDriverRef,
        LayerCacheVlmDbDriver layerCacheVlmDbDriverRef,
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
            rscLayerIdDriverRef,
            null,
            null,
            layerCacheVlmDbDriverRef,
            transObjFactoryRef,
            transMgrProviderRef
        );
        layerCacheVlmDbDriver = layerCacheVlmDbDriverRef;
    }

    @Override
    public DeviceLayerKind getDeviceLayerKind()
    {
        return DeviceLayerKind.CACHE;
    }

    @Override
    protected <RSC extends AbsResource<RSC>> RscDataLoadOutput<CacheRscData<?>, CacheVlmData<?>> loadImpl(
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
        Map<VolumeNumber, CacheVlmData<?>> vlmMap = new HashMap<>();
        Set<AbsRscLayerObject<?>> childrenSet = new HashSet<>();

        CacheRscData<?> rscData = genericCreate(
            absRscLayerObject,
            loadedParentRscDataRef,
            absRscRef,
            childrenSet,
            vlmMap
        );
        return new RscDataLoadOutput<>(rscData, childrenSet, vlmMap);
    }

    @SuppressWarnings("unchecked")
    private <RSC extends AbsResource<RSC>> CacheRscData<RSC> genericCreate(
        AbsRscLayerObject<?> absRscLayerObjectRef,
        @Nullable AbsRscLayerObject<?> loadedParentRscDataRef,
        RSC absRscRef,
        Set<AbsRscLayerObject<?>> childrenSetRef,
        Map<VolumeNumber, CacheVlmData<?>> vlmMapRef
    )
    {
        Object typelessVlmMap = vlmMapRef;
        Object typelessChildrenSet = childrenSetRef;
        return new CacheRscData<>(
            absRscLayerObjectRef.getRscLayerId(),
            absRscRef,
            (AbsRscLayerObject<RSC>) loadedParentRscDataRef,
            (Set<AbsRscLayerObject<RSC>>) typelessChildrenSet,
            absRscLayerObjectRef.getResourceNameSuffix(),
            this,
            layerCacheVlmDbDriver,
            (Map<VolumeNumber, CacheVlmData<RSC>>) typelessVlmMap,
            transObjFactory,
            transMgrProvider
        );
    }
}
