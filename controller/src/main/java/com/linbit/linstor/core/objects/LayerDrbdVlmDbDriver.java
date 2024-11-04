package com.linbit.linstor.core.objects;

import com.linbit.ExhaustedPoolException;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsLayerRscDataDbDriver.SuffixedResourceName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LayerDrbdVolumes;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.LayerDrbdVlmDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmDfnData;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgrSQL;
import com.linbit.utils.Pair;
import com.linbit.utils.PairNonNull;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.function.Function;

@Singleton
public class LayerDrbdVlmDbDriver
    extends AbsLayerVlmDataDbDriver<DrbdVlmDfnData<?>, DrbdRscData<?>, DrbdVlmData<?>>
    implements LayerDrbdVlmDatabaseDriver
{
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgrSQL> transMgrProvider;

    private final LayerResourceIdDatabaseDriver rscLayerIdDbDriver;
    private final SingleColumnDatabaseDriver<DrbdVlmData<?>, StorPool> extStorPoolDriver;

    @Inject
    public LayerDrbdVlmDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        LayerResourceIdDatabaseDriver rscLayerIdDbDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgrSQL> transMgrProviderRef
    )
    {
        super(dbCtxRef, errorReporterRef, GeneratedDatabaseTables.LAYER_DRBD_VOLUMES, dbEngineRef);
        rscLayerIdDbDriver = rscLayerIdDbDriverRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;

        setColumnSetter(LayerDrbdVolumes.LAYER_RESOURCE_ID, DrbdVlmData::getRscLayerId);
        setColumnSetter(LayerDrbdVolumes.VLM_NR, drbdVlmData -> drbdVlmData.getVlmNr().value);
        setColumnSetter(
            LayerDrbdVolumes.NODE_NAME,
            drbdVlmData -> drbdVlmDataToExtStorPool(drbdVlmData, this::storPoolToNodeNameStr)
        );
        setColumnSetter(
            LayerDrbdVolumes.POOL_NAME,
            drbdVlmData -> drbdVlmDataToExtStorPool(drbdVlmData, this::storPoolToSpNameStr)
        );

        /*
         * Although the externalMetadataDatabaseDriver is used everywhere as a SingleColumnDatabaseDriver
         * in fact this driver has to update 2 columns instead of one. To solve this incompatibility
         * we manually create a dummy "Single"-ColumnDatabaseDriver that calls 2 actual (generated)
         * SingleColumnDatabaseDrivers
         */
        SingleColumnDatabaseDriver<DrbdVlmData<?>, StorPool> extSpToNodeNameDriver = this
            .<StorPool, String>generateSingleColumnDriver(
            LayerDrbdVolumes.NODE_NAME,
                drbdVlmData -> drbdVlmDataToExtStorPool(drbdVlmData, this::storPoolToNodeNameStr),
                this::storPoolToNodeNameStr
        );
        SingleColumnDatabaseDriver<DrbdVlmData<?>, StorPool> extSpToSpNameDriver = this
            .<StorPool, String>generateSingleColumnDriver(
                LayerDrbdVolumes.POOL_NAME,
                drbdVlmData -> drbdVlmDataToExtStorPool(drbdVlmData, this::storPoolToSpNameStr),
                this::storPoolToSpNameStr
            );

        extStorPoolDriver = (drbdVlmData, extStorPool) -> {
            extSpToNodeNameDriver.update(drbdVlmData, extStorPool);
            extSpToSpNameDriver.update(drbdVlmData, extStorPool);
        };
    }

    private String drbdVlmDataToExtStorPool(DrbdVlmData<?> drbdVlmData, Function<StorPool, String> spToStrFunc)
    {
        return spToStrFunc.apply(drbdVlmData.getExternalMetaDataStorPool());
    }

    private String storPoolToNodeNameStr(StorPool extSp)
    {
        String ret = null;
        if (extSp != null)
        {
            ret = extSp.getNode().getName().value;
        }
        return ret;
    }

    private String storPoolToSpNameStr(StorPool extSp)
    {
        String ret = null;
        if (extSp != null)
        {
            ret = extSp.getName().value;
        }
        return ret;
    }

    @Override
    public LayerResourceIdDatabaseDriver getIdDriver()
    {
        return rscLayerIdDbDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<DrbdVlmData<?>, StorPool> getExtStorPoolDriver()
    {
        return extStorPoolDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Pair<DrbdVlmData<?>, Void> load(
        RawParameters rawRef,
        VlmParentObjects<DrbdVlmDfnData<?>, DrbdRscData<?>, DrbdVlmData<?>> loadedParentObjectsRef
    )
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException,
        MdException, ExhaustedPoolException, ValueInUseException, RuntimeException, AccessDeniedException
    {
        int lri = rawRef.getParsed(LayerDrbdVolumes.LAYER_RESOURCE_ID);
        VolumeNumber vlmNr = rawRef.buildParsed(LayerDrbdVolumes.VLM_NR, VolumeNumber::new);
        NodeName extStorPoolNodeName = rawRef.build(LayerDrbdVolumes.NODE_NAME, NodeName::new);
        StorPoolName extStorPoolName = rawRef.build(LayerDrbdVolumes.POOL_NAME, StorPoolName::new);

        DrbdRscData<?> drbdRscData = loadedParentObjectsRef.getRscData(lri);
        AbsResource<?> absResource = drbdRscData.getAbsResource();

        PairNonNull<NodeName, SuffixedResourceName> nodeNameSuffixedRscNamePair = AbsLayerRscDataDbDriver
            .getNodeNameSuffixedRscNamePair(drbdRscData);

        StorPool extMetaDataStorPool = null;
        if (extStorPoolNodeName != null && extStorPoolName != null)
        {
            extMetaDataStorPool = loadedParentObjectsRef.storPoolWithInitMap.get(
                new PairNonNull<>(
                    extStorPoolNodeName,
                    extStorPoolName
                )
            ).objA;
        }

        DrbdVlmData<?> drbdVlmData;
        if (absResource instanceof Resource)
        {
            drbdVlmData = new DrbdVlmData<>(
                (Volume) absResource.getVolume(vlmNr),
                (DrbdRscData<Resource>) ((Object) drbdRscData),
                (DrbdVlmDfnData<Resource>) loadedParentObjectsRef.vlmDfnDataMap
                    .get(new Pair<>(nodeNameSuffixedRscNamePair.objB, vlmNr)),
                extMetaDataStorPool,
                this,
                transObjFactory,
                transMgrProvider
            );
        }
        else
        {
            drbdVlmData = new DrbdVlmData<>(
                (SnapshotVolume) absResource.getVolume(vlmNr),
                (DrbdRscData<Snapshot>) ((Object) drbdRscData),
                (DrbdVlmDfnData<Snapshot>) loadedParentObjectsRef.vlmDfnDataMap
                    .get(new Pair<>(nodeNameSuffixedRscNamePair.objB, vlmNr)),
                extMetaDataStorPool,
                this,
                transObjFactory,
                transMgrProvider
            );
        }
        return new Pair<>(drbdVlmData, null);
    }

    @Override
    protected String getId(DrbdVlmData<?> drbdVlmData) throws AccessDeniedException
    {
        return "(LayerRscId=" + drbdVlmData.getRscLayerId() +
            ", VlmNr=" + drbdVlmData.getVlmNr() + ")";
    }
}
