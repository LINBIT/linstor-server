package com.linbit.linstor.core.objects;

import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.LayerOpenflexRscCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.storage.data.adapter.nvme.OpenflexRscData;
import com.linbit.linstor.storage.data.adapter.nvme.OpenflexRscDfnData;
import com.linbit.linstor.storage.data.adapter.nvme.OpenflexVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
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
public class LayerOpenflexRscDbDriver
    extends AbsLayerRscDataDbDriver<OpenflexRscDfnData<?>, VlmDfnLayerObject, OpenflexRscData<?>, OpenflexVlmData<?>>
    implements LayerOpenflexRscCtrlDatabaseDriver
{
    private final LayerOpenflexVlmDbDriver openflexVlmDbDriver;

    @Inject
    public LayerOpenflexRscDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        ObjectProtectionFactory objProtFactoryRef,
        LayerResourceIdDatabaseDriver rscLayerIdDriverRef,
        LayerOpenflexVlmDbDriver openflexVlmDbDriverRef,
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
            null,
            transObjFactoryRef,
            transMgrProviderRef
        );
        openflexVlmDbDriver = openflexVlmDbDriverRef;
    }

    @Override
    public DeviceLayerKind getDeviceLayerKind()
    {
        return DeviceLayerKind.OPENFLEX;
    }

    @Override
    protected <RSC extends AbsResource<RSC>> RscDataLoadOutput<OpenflexRscData<?>, OpenflexVlmData<?>> loadImpl(
        RawParameters rawRef,
        ParentObjects parentRef,
        AbsRscLayerObject<?> currentDummyLoadingRLORef,
        Pair<NodeName, SuffixedResourceName> nodeNameSuffixedRscNamePairRef,
        AbsRscLayerObject<?> loadedParentRscDataRef,
        RSC absRscRef
    )
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException, MdException
    {
        AbsRscLayerObject<?> absRscLayerObject = rawRef.get(NULL_TABLE_LAYER_RSC_DATA_COLUMN);
        Map<VolumeNumber, OpenflexVlmData<?>> vlmMap = new HashMap<>();
        Set<AbsRscLayerObject<?>> childrenSet = new HashSet<>();

        SuffixedResourceName suffixedRscName = new SuffixedResourceName(
            absRscLayerObject.getResourceName(),
            absRscLayerObject.getSnapName(),
            absRscLayerObject.getResourceNameSuffix()
        );

        OpenflexRscData<?> rscData = genericCreate(
            absRscLayerObject,
            loadedParentRscDataRef,
            getRscDfnData(suffixedRscName),
            absRscRef,
            childrenSet,
            vlmMap
        );
        return new RscDataLoadOutput<>(rscData, childrenSet, vlmMap);
    }

    @SuppressWarnings("unchecked")
    private <RSC extends AbsResource<RSC>> OpenflexRscData<RSC> genericCreate(
        AbsRscLayerObject<?> absRscLayerObjectRef,
        AbsRscLayerObject<?> loadedParentRscDataRef,
        OpenflexRscDfnData<?> openflexRscDfnDataRef,
        RSC absRscRef,
        Set<AbsRscLayerObject<?>> childrenSetRef,
        Map<VolumeNumber, OpenflexVlmData<?>> vlmMapRef
    )
    {
        Object typelessVlmMap = vlmMapRef;
        Map<VolumeNumber, OpenflexVlmData<RSC>> typedVlmMap = (Map<VolumeNumber, OpenflexVlmData<RSC>>) typelessVlmMap;

        Object typelessChildrenSet = childrenSetRef;
        Set<AbsRscLayerObject<RSC>> typedChildrenSet = (Set<AbsRscLayerObject<RSC>>) typelessChildrenSet;

        return new OpenflexRscData<>(
            absRscLayerObjectRef.getRscLayerId(),
            absRscRef,
            (OpenflexRscDfnData<RSC>) openflexRscDfnDataRef,
            (AbsRscLayerObject<RSC>) loadedParentRscDataRef,
            typedChildrenSet,
            typedVlmMap,
            this,
            openflexVlmDbDriver,
            transObjFactory,
            transMgrProvider
        );
    }
}
