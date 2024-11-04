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
import com.linbit.linstor.dbdrivers.interfaces.LayerNvmeRscCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeRscData;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgrSQL;
import com.linbit.utils.PairNonNull;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

@Singleton
public class LayerNvmeRscDbDriver
    extends AbsLayerRscDataDbDriver<RscDfnLayerObject, VlmDfnLayerObject, NvmeRscData<?>, NvmeVlmData<?>>
    implements LayerNvmeRscCtrlDatabaseDriver
{
    @Inject
    public LayerNvmeRscDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        LayerResourceIdDatabaseDriver rscLayerIdDriverRef,
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
            null,
            transObjFactoryRef,
            transMgrProviderRef
        );
    }

    @Override
    public DeviceLayerKind getDeviceLayerKind()
    {
        return DeviceLayerKind.NVME;
    }

    @Override
    protected <RSC extends AbsResource<RSC>> RscDataLoadOutput<NvmeRscData<?>, NvmeVlmData<?>> loadImpl(
        RawParameters rawRef,
        ParentObjects parentRef,
        AbsRscLayerObject<?> currentDummyLoadingRLORef,
        PairNonNull<NodeName, SuffixedResourceName> nodeNameSuffixedRscNamePairRef,
        @Nullable AbsRscLayerObject<?> loadedParentRscDataRef,
        RSC absRscRef
    )
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException, MdException
    {
        AbsRscLayerObject<?> absRscLayerObject = rawRef.get(NULL_TABLE_LAYER_RSC_DATA_COLUMN);
        Map<VolumeNumber, NvmeVlmData<?>> vlmMap = new HashMap<>();
        Set<AbsRscLayerObject<?>> childrenSet = new HashSet<>();

        /*
         * We do not have a LayerNvmeVolumes database table. That leads to two things:
         * 1) we do not know which NvmeVlmData we need to create/restore and which not -> so we create for all AbsVolume
         * an NvmeVlmData (see issue 835 for more information)
         * 2) we do not need a LayerNvmeVlmDbDriver - for now. As soon as we create a DB table where we store which
         * NvmeVlmData we need to restore (which could be simply the rscId of the NvmeRscData and the volume number), we
         * would also need a LayerNvmeVlmDbDriver.
         *
         * If we create a LayerNvmeVolumes database table, make sure to also update LayerDataFactory#createNvmeVlmData
         * method to persist the newly created NvmeVlmData
         *
         * The NvmeVlmData are created as as part of the genericCreate method
         */

        NvmeRscData<?> rscData = genericCreate(
            absRscLayerObject,
            loadedParentRscDataRef,
            absRscRef,
            childrenSet,
            vlmMap
        );

        return new RscDataLoadOutput<>(rscData, childrenSet, vlmMap);
    }

    @SuppressWarnings("unchecked")
    private <RSC extends AbsResource<RSC>> NvmeRscData<RSC> genericCreate(
        AbsRscLayerObject<?> absRscLayerObjectRef,
        @Nullable AbsRscLayerObject<?> loadedParentRscDataRef,
        RSC absRscRef,
        Set<AbsRscLayerObject<?>> childrenSetRef,
        Map<VolumeNumber, NvmeVlmData<?>> vlmMapRef
    )
    {
        Object typelessVlmMap = vlmMapRef;
        Map<VolumeNumber, NvmeVlmData<RSC>> typedVlmMap = (Map<VolumeNumber, NvmeVlmData<RSC>>) typelessVlmMap;

        Object typelessChildrenSet = childrenSetRef;
        Set<AbsRscLayerObject<RSC>> typedChildrenSet = (Set<AbsRscLayerObject<RSC>>) typelessChildrenSet;

        NvmeRscData<RSC> nvmeRscData = new NvmeRscData<>(
            absRscLayerObjectRef.getRscLayerId(),
            absRscRef,
            (AbsRscLayerObject<RSC>) loadedParentRscDataRef,
            typedChildrenSet,
            typedVlmMap,
            absRscLayerObjectRef.getResourceNameSuffix(),
            this,
            transObjFactory,
            transMgrProvider
        );

        /*
         * Create also NvmeVlmData for all absVolumes since we do not know better
         */
        Iterator<? extends AbsVolume<RSC>> vlmsIt = absRscRef.iterateVolumes();
        while (vlmsIt.hasNext())
        {
            AbsVolume<RSC> absVolume = vlmsIt.next();
            vlmMapRef.put(
                absVolume.getVolumeNumber(),
                new NvmeVlmData<>(absVolume, nvmeRscData, transObjFactory, transMgrProvider)
            );
        }

        return nvmeRscData;
    }
}
