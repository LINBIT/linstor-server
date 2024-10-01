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
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerStorageRscCtrlDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgrSQL;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.HashMap;
import java.util.Map;

@Singleton
public class LayerStorageRscDbDriver
    extends AbsLayerRscDataDbDriver<RscDfnLayerObject, VlmDfnLayerObject, StorageRscData<?>, VlmProviderObject<?>>
    implements LayerStorageRscCtrlDatabaseDriver
{
    private final LayerStorageVlmDbDriver layerStorageVlmDbDriver;

    @Inject
    public LayerStorageRscDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        LayerResourceIdDatabaseDriver rscLayerIdDriverRef,
        LayerStorageVlmDbDriver layerStorageVlmDbDriverRef,
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
            layerStorageVlmDbDriverRef,
            transObjFactoryRef,
            transMgrProviderRef
        );
        layerStorageVlmDbDriver = layerStorageVlmDbDriverRef;
    }

    @Override
    public DeviceLayerKind getDeviceLayerKind()
    {
        return DeviceLayerKind.STORAGE;
    }

    @Override
    protected <RSC extends AbsResource<RSC>> RscDataLoadOutput<StorageRscData<?>, VlmProviderObject<?>> loadImpl(
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
        Map<VolumeNumber, VlmProviderObject<?>> vlmMap = new HashMap<>();

        StorageRscData<?> rscData = genericCreate(
            absRscLayerObject,
            loadedParentRscDataRef,
            absRscRef,
            vlmMap
        );
        return new RscDataLoadOutput<>(rscData, null, vlmMap);
    }

    @SuppressWarnings("unchecked")
    private <RSC extends AbsResource<RSC>> StorageRscData<RSC> genericCreate(
        AbsRscLayerObject<?> absRscLayerObjectRef,
        @Nullable AbsRscLayerObject<?> loadedParentRscDataRef,
        RSC absRscRef,
        Map<VolumeNumber, VlmProviderObject<?>> vlmMapRef
    )
    {
        Object typelessVlmMap = vlmMapRef;
        Map<VolumeNumber, VlmProviderObject<RSC>> storVlmDataMap =
            (Map<VolumeNumber, VlmProviderObject<RSC>>) typelessVlmMap;

        return new StorageRscData<>(
            absRscLayerObjectRef.getRscLayerId(),
            (AbsRscLayerObject<RSC>) loadedParentRscDataRef,
            absRscRef,
            absRscLayerObjectRef.getResourceNameSuffix(),
            storVlmDataMap,
            this,
            layerStorageVlmDbDriver,
            transObjFactory,
            transMgrProvider
        );
    }
}
