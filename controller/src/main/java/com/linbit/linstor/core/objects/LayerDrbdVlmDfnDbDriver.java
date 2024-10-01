package com.linbit.linstor.core.objects;

import com.linbit.ExhaustedPoolException;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsLayerRscDataDbDriver.SuffixedResourceName;
import com.linbit.linstor.core.types.MinorNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LayerDrbdVolumeDefinitions;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.LayerDrbdVlmDfnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmDfnData;
import com.linbit.linstor.transaction.manager.TransactionMgrSQL;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

@Singleton
public class LayerDrbdVlmDfnDbDriver
    extends AbsLayerVlmDfnDataDbDriver<DrbdRscDfnData<?>, DrbdVlmDfnData<?>>
    implements LayerDrbdVlmDfnDatabaseDriver
{
    private final DynamicNumberPool minorPool;
    private final Provider<TransactionMgrSQL> transMgrProvider;

    @Inject
    public LayerDrbdVlmDfnDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        @Named(NumberPoolModule.MINOR_NUMBER_POOL) DynamicNumberPool minorPoolRef,
        Provider<TransactionMgrSQL> transMgrProviderRef
    )
    {
        super(
            dbCtxRef,
            errorReporterRef,
            GeneratedDatabaseTables.LAYER_DRBD_VOLUME_DEFINITIONS,
            dbEngineRef
        );
        minorPool = minorPoolRef;
        transMgrProvider = transMgrProviderRef;

        setColumnSetter(
            LayerDrbdVolumeDefinitions.RESOURCE_NAME,
            drbdVlmDfnData -> drbdVlmDfnData.getResourceName().value
        );
        setColumnSetter(LayerDrbdVolumeDefinitions.RESOURCE_NAME_SUFFIX, DrbdVlmDfnData::getRscNameSuffix);
        setColumnSetter(
            LayerDrbdVolumeDefinitions.SNAPSHOT_NAME,
            drbdVlmDfnData ->
            {
                String ret;
                SnapshotName snapName = drbdVlmDfnData.getSnapshotName();
                if (snapName != null)
                {
                    ret = snapName.value;
                }
                else
                {
                    ret = ResourceDefinitionDbDriver.DFLT_SNAP_NAME_FOR_RSC;
                }
                return ret;
            }
        );
        setColumnSetter(LayerDrbdVolumeDefinitions.VLM_NR, drbdVlmDfnData -> drbdVlmDfnData.getVolumeNumber().value);
        setColumnSetter(LayerDrbdVolumeDefinitions.VLM_MINOR_NR, drbdVlmDfnData ->
        {
            MinorNumber minorNr = drbdVlmDfnData.getMinorNr();
            Integer value = null;
            if (minorNr != null)
            {
                value = minorNr.value;
            }
            return value;
        });
    }

    @Override
    protected Pair<DrbdVlmDfnData<?>, Void> load(
        RawParameters raw,
        AbsLayerVlmDfnDataDbDriver.VlmDfnParentObjects<DrbdRscDfnData<?>> parentRef
    )
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException,
        MdException, AccessDeniedException, ExhaustedPoolException, ValueInUseException
    {
        DrbdVlmDfnData<?> drbdVlmDfnData;

        ResourceName rscName = raw.buildParsed(LayerDrbdVolumeDefinitions.RESOURCE_NAME, ResourceName::new);
        VolumeNumber vlmNr = raw.buildParsed(LayerDrbdVolumeDefinitions.VLM_NR, VolumeNumber::new);
        Integer minorNr = raw.getParsed(LayerDrbdVolumeDefinitions.VLM_MINOR_NR);
        String rscNameSuffix = raw.get(LayerDrbdVolumeDefinitions.RESOURCE_NAME_SUFFIX);
        String snapNameStr = raw.get(LayerDrbdVolumeDefinitions.SNAPSHOT_NAME);

        VolumeDefinition vlmDfn = parentRef.rscDfnMap.get(rscName).getVolumeDfn(dbCtx, vlmNr);

        SuffixedResourceName suffixedResourceName;

        if (snapNameStr == null || snapNameStr.equals(ResourceDefinitionDbDriver.DFLT_SNAP_NAME_FOR_RSC))
        {
            suffixedResourceName = new SuffixedResourceName(rscName, null, rscNameSuffix);
            if (minorNr == null)
            {
                throw new DatabaseException("DrbdVlmDfnData without minorNumber!");
            }
        }
        else
        {
            suffixedResourceName = new SuffixedResourceName(rscName, new SnapshotName(snapNameStr), rscNameSuffix);
            minorNr = DrbdVlmDfnData.SNAPSHOT_MINOR;
        }
        drbdVlmDfnData = this.genericCreate(vlmDfn, suffixedResourceName, vlmNr, minorNr, parentRef);

        return new Pair<>(drbdVlmDfnData, null);
    }

    @SuppressWarnings("unchecked")
    private <RSC extends AbsResource<RSC>> DrbdVlmDfnData<?> genericCreate(
        VolumeDefinition vlmDfnRef,
        SuffixedResourceName suffixedResourceNameRef,
        VolumeNumber vlmNrRef,
        Integer minorNrRef,
        VlmDfnParentObjects<DrbdRscDfnData<?>> parentRef
    )
        throws ValueOutOfRangeException, ExhaustedPoolException, ValueInUseException
    {
        return new DrbdVlmDfnData<>(
            vlmDfnRef,
            suffixedResourceNameRef.rscName,
            suffixedResourceNameRef.snapName,
            suffixedResourceNameRef.rscNameSuffix,
            vlmNrRef,
            minorNrRef,
            minorPool,
            (DrbdRscDfnData<RSC>) parentRef.rscDfnDataMap.get(suffixedResourceNameRef),
            this,
            transMgrProvider
        );
    }

}
