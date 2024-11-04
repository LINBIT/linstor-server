package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsLayerRscDataDbDriver.ParentObjects;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.ControllerLayerRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseDriverInfo.DatabaseType;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.DatabaseTable.Column;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgrSQL;
import com.linbit.utils.Pair;
import com.linbit.utils.PairNonNull;
import com.linbit.utils.Triple;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Objects;

public abstract class AbsLayerRscDataDbDriver<
        RSC_DFN_DATA extends RscDfnLayerObject,
        VLM_DFN_DATA extends VlmDfnLayerObject,
        RSC_DATA extends AbsRscLayerObject<?>,
    VLM_DATA extends VlmProviderObject<?>>
    extends AbsDatabaseDriver<
        RSC_DATA,
        Pair<Set<AbsRscLayerObject<?>>, Map<VolumeNumber, VLM_DATA>>,
        ParentObjects>
    implements ControllerLayerRscDatabaseDriver
{
    public static class SuffixedResourceName implements Comparable<SuffixedResourceName>
    {
        final ResourceName rscName;
        final @Nullable SnapshotName snapName;
        final String rscNameSuffix;

        public SuffixedResourceName(
            ResourceName resourceNameRef,
            @Nullable SnapshotName snapshotNameRef,
            String rscNameSuffixRef
        )
        {
            rscName = resourceNameRef;
            snapName = snapshotNameRef;
            rscNameSuffix = rscNameSuffixRef;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + rscName.hashCode();
            result = prime * result + rscNameSuffix.hashCode();
            result = prime * result + ((snapName == null) ? 0 : snapName.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            boolean ret = false;
            if (obj instanceof SuffixedResourceName)
            {
                SuffixedResourceName other = (SuffixedResourceName) obj;
                ret = Objects.equal(rscName, other.rscName) &&
                    Objects.equal(snapName, other.snapName) &&
                    Objects.equal(rscNameSuffix, other.rscNameSuffix);
            }
            return ret;
        }

        @Override
        public int compareTo(SuffixedResourceName oRef)
        {
            int cmp = rscName.compareTo(oRef.rscName);
            if (cmp == 0)
            {
                if (snapName != null)
                {
                    cmp = snapName.compareTo(oRef.snapName);
                }
                else
                {
                    if (oRef.snapName != null)
                    {
                        cmp = 1;
                    }
                }
                if (cmp == 0)
                {
                    cmp = rscNameSuffix.compareTo(oRef.rscNameSuffix);
                }
            }
            return cmp;
        }
    }

    public static class ParentObjects
    {
        /**
         * Values are only instances of {@link LayerResourceIdDbDriver.ResourceLayerIdLoadingPojo}.
         * These values are only used as holder classes of the database rows. These values will
         * throw {@link ImplementationError} if any other get method is called that these pojos
         * have no information about.
         */
        final Map<Integer, AbsRscLayerObject<?>> dummyLayerResourceObjectyById;
        /**
         * The values of this map are the already loaded RscLayerObjects that can and should be
         * used as parent objects in order to build the layer tree
         */
        final Map<Integer, AbsRscLayerObject<?>> loadedLayerResourceObjectyById;

        final Map<Pair<NodeName, ResourceName>, Resource> rscMap;
        final Map<ResourceName, ResourceDefinition> rscDfnMap;
        final Map<Triple<NodeName, ResourceName, SnapshotName>, Snapshot> snapMap;
        final Map<Pair<ResourceName, SnapshotName>, SnapshotDefinition> snapDfnMap;

        final Map<PairNonNull<NodeName, StorPoolName>, PairNonNull<StorPool, StorPool.InitMaps>> storPoolWithInitMap;

        public ParentObjects(
            Map<Integer, AbsRscLayerObject<?>> dummyLayerResourceObjectyByIdRef,
            Map<Pair<NodeName, ResourceName>, Resource> rscMapRef,
            Map<ResourceName, ResourceDefinition> rscDfnMapRef,
            Map<Triple<NodeName, ResourceName, SnapshotName>, Snapshot> snapMapRef,
            Map<Pair<ResourceName, SnapshotName>, SnapshotDefinition> snapDfnMapRef,
            Map<PairNonNull<NodeName, StorPoolName>, PairNonNull<StorPool, StorPool.InitMaps>> storPoolWithInitMapRef
        )
        {
            dummyLayerResourceObjectyById = dummyLayerResourceObjectyByIdRef;
            rscMap = rscMapRef;
            rscDfnMap = rscDfnMapRef;
            snapMap = snapMapRef;
            snapDfnMap = snapDfnMapRef;
            storPoolWithInitMap = storPoolWithInitMapRef;

            loadedLayerResourceObjectyById = new HashMap<>();
        }

        public List<AbsRscLayerObject<?>> getAllRloPojoList()
        {
            return new ArrayList<>(dummyLayerResourceObjectyById.values());
        }
    }

    static class RscDataLoadOutput<
        RSC_DATA_LOAD extends AbsRscLayerObject<?>,
        VLM_DATA_LOAD extends VlmProviderObject<?>>
    {
        final RSC_DATA_LOAD rscData;
        final @Nullable Set<AbsRscLayerObject<?>> rscDataChildren;
        final Map<VolumeNumber, VLM_DATA_LOAD> vlmDataMap;

        RscDataLoadOutput(
            RSC_DATA_LOAD rscDataRef,
            @Nullable Set<AbsRscLayerObject<?>> rscDataChildrenRef,
            Map<VolumeNumber, VLM_DATA_LOAD> vlmDataMapRef
        )
        {
            rscData = rscDataRef;
            rscDataChildren = rscDataChildrenRef;
            vlmDataMap = vlmDataMapRef;
        }
    }

    /**
     * A special column that is used to populate the RawParameter in case the current (this) *RscDbDriver does not have
     * a table on its own (like Storage layer, which only has a table for the volumes, not the resources).
     * The RawParameters will be populated with only one entry, with this NULL_TABLE_LAYER_RSC_ID_COLUMN as key and the
     * actual {@link LayerResourceIdDbDriver.ResourceLayerIdLoadingPojo} as value
     */
    static final NullTableLayerRscDataColumn NULL_TABLE_LAYER_RSC_DATA_COLUMN = new NullTableLayerRscDataColumn();

    protected final AccessContext dbCtx;

    protected final TransactionObjectFactory transObjFactory;
    protected final Provider<TransactionMgrSQL> transMgrProvider;

    private final LayerResourceIdDatabaseDriver rscLayerIdDriver;
    private final @Nullable AbsLayerRscDfnDataDbDriver<RSC_DFN_DATA, RSC_DATA> rscDfnDriver;
    private final @Nullable AbsLayerVlmDfnDataDbDriver<RSC_DFN_DATA, VLM_DFN_DATA> vlmDfnDriver;
    private final @Nullable AbsLayerVlmDataDbDriver<VLM_DFN_DATA, RSC_DATA, VLM_DATA> vlmDriver;

    private final @Nullable Column layerRscIdColumn;
    private final Map<Integer, RawParameters> rscDataRawCache;

    private final Map<SuffixedResourceName, RSC_DFN_DATA> allRscDfnData = new HashMap<>();
    private final Map<Pair<SuffixedResourceName, VolumeNumber>, VLM_DFN_DATA> allVlmDfnData = new HashMap<>();

    private @Nullable ParentObjects parentObjects;
    private final Map<Integer, RscDataLoadOutput<RSC_DATA, VLM_DATA>> rscDataByLayerId = new HashMap<>();

    private final Map<RSC_DFN_DATA, List<RSC_DATA>> loadedRscDfnChildRscDataMap = new HashMap<>();


    protected AbsLayerRscDataDbDriver(
        AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        @Nullable DatabaseTable tableRef,
        @Nullable Column layerRscIdColumnRef,
        DbEngine dbEngineRef,
        LayerResourceIdDatabaseDriver rscLayerIdDriverRef,
        @Nullable AbsLayerRscDfnDataDbDriver<RSC_DFN_DATA, RSC_DATA> rscDfnDriverRef,
        @Nullable AbsLayerVlmDfnDataDbDriver<RSC_DFN_DATA, VLM_DFN_DATA> vlmDfnDriverRef,
        @Nullable AbsLayerVlmDataDbDriver<VLM_DFN_DATA, RSC_DATA, VLM_DATA> vlmDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgrSQL> transMgrProviderRef
    )
    {
        super(dbCtxRef, errorReporterRef, tableRef, dbEngineRef);
        dbCtx = dbCtxRef;
        layerRscIdColumn = layerRscIdColumnRef;
        rscLayerIdDriver = rscLayerIdDriverRef;
        rscDfnDriver = rscDfnDriverRef;
        vlmDfnDriver = vlmDfnDriverRef;
        vlmDriver = vlmDriverRef;

        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;

        rscDataRawCache = new HashMap<>();
    }

    @Override
    public void clearLoadingCaches()
    {
        rscDataRawCache.clear();
        loadedRscDfnChildRscDataMap.clear();
    }

    @Override
    public void cacheAll(ParentObjects parentObjectsRef) throws DatabaseException
    {
        try
        {
            parentObjects = parentObjectsRef;
            if (rscDfnDriver != null)
            {
                rscDfnDriver.cacheAll(parentObjectsRef, loadedRscDfnChildRscDataMap, allRscDfnData);
            }

            if (vlmDfnDriver != null)
            {
                vlmDfnDriver.cacheAll(parentObjectsRef, allRscDfnData, allVlmDfnData);
            }

            if (table != null)
            {
                super.loadAll(parentObjectsRef);
            }
            else
            {
                /*
                 * The current DbDriver does not have a dedicated database table for its resources (like the storage
                 * layer).
                 * However, not calling super.loadAll will not populate the rscDataRawCache, and would leave calling the
                 * loadImpl method with a null value instead of its RawParameters
                 *
                 * To prevent this, we now populate the rscDataRawCache with the basic information from the
                 * LayerResourceId table,
                 */
                DeviceLayerKind localKind = getDeviceLayerKind();
                DatabaseType dbType = getDbType();
                for (AbsRscLayerObject<?> absRscLayerObject : parentObjectsRef.dummyLayerResourceObjectyById.values())
                {
                    if (absRscLayerObject.getLayerKind().equals(localKind))
                    {
                        Map<String, Object> rawMap = new HashMap<>();

                        rawMap.put(NULL_TABLE_LAYER_RSC_DATA_COLUMN.getName(), absRscLayerObject);

                        rscDataRawCache.put(absRscLayerObject.getRscLayerId(), new RawParameters(null, rawMap, dbType));
                    }
                }
            }

            // intentionally not loading volumes here.
            // volumes are loaded once all layerRscData (from all layers, not just the current) are loaded, since
            // volumes require their rscData, which on the other hand has to be loaded in a given order (due to
            // parent-child dependency).

            // volumes are loaded at the end, all volumes from each table
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    @Override
    public void loadAllLayerVlmData() throws DatabaseException
    {
        if (vlmDriver != null)
        {
            vlmDriver.loadAll(allVlmDfnData, rscDataByLayerId, parentObjects);
        }
    }

    /*
     * Loads the data from the database, but does not create layer data instances as we have to cache
     * all layers since the order of layers might lead to chicken-egg problem (i.e. cache,writecache is just
     * as valid as writecache,cache. It is not possible to fully load and create instances from one of the DB tables
     * entirely before the other)
     */
    @Override
    protected @Nullable Pair<RSC_DATA, Pair<Set<AbsRscLayerObject<?>>, Map<VolumeNumber, VLM_DATA>>> load(
        RawParameters rawRef,
        ParentObjects parentRef
    )
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException, MdException
    {
        /*
         * returning null will cause the DbEngine to skip the entry. This is already in use when loading RscDfn and
         * SnapDfn, which are in the same database table, but will end up in either the rscDfnMap or the snapDfnMap. If
         * the loaded *Dfn has the SnapshotName column set (i.e. not empty) but the driver wants to load
         * ResourceDefinitions, the driver returns null, so no snapDfn is tried to put into a map of rscDfns.
         */
        /*
         * In this case we return null to skip all entries of the given database table, but build an internal
         * cache of the database-rows (indexed by rscLayerId)
         */
        int lri;
        switch (getDbType())
        {
            case SQL:
            case K8S_CRD:
                lri = rawRef.get(layerRscIdColumn);
                break;
            case ETCD:
                lri = rawRef.etcdGetInt(layerRscIdColumn);
                break;
            default:
                throw new ImplementationError("Unknown db type: " + getDbType());
        }

        rscDataRawCache.put(lri, rawRef);

        return null;
    }

    @Override
    protected int getLoadedCount(
        Map<RSC_DATA, Pair<Set<AbsRscLayerObject<?>>, Map<VolumeNumber, VLM_DATA>>> ignoredMapRef
    )
    {
        return rscDataRawCache.size();
    }

    /**
     * Actually create an instance from the cached database row
     */
    @SuppressWarnings("unchecked")
    @Override
    public <RSC extends AbsResource<RSC>> Pair<? extends AbsRscLayerObject<RSC>, Set<AbsRscLayerObject<RSC>>> load(
        RSC absRscRef,
        int rscLayerIdRef
    )
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException, MdException
    {
        AbsRscLayerObject<?> dummyLoadingRLO = getDummyLoadingRLO(rscLayerIdRef);
        PairNonNull<NodeName, SuffixedResourceName> nodeNameSuffixedRscNamePair = getNodeNameSuffixedRscNamePair(
            dummyLoadingRLO
        );

        if (absRscRef instanceof Resource)
        {
            if (nodeNameSuffixedRscNamePair.objB.snapName != null)
            {
                throw new DatabaseException(
                    String.format(
                        "Resource %s on node %s loaded with id %d the snapshot %s",
                        absRscRef.getResourceDefinition().getName().displayValue,
                        absRscRef.getNode().getName(),
                        rscLayerIdRef,
                        nodeNameSuffixedRscNamePair.objB.snapName
                    )
                );
            }
        }
        else
        {
            if (nodeNameSuffixedRscNamePair.objB.snapName == null)
            {
                throw new DatabaseException(
                    String.format(
                        "Snapshot %s of resource %s on node %s loaded with id %d its resource",
                        ((Snapshot) absRscRef).getSnapshotName().displayValue,
                        absRscRef.getResourceDefinition().getName().displayValue,
                        absRscRef.getNode().getName(),
                        rscLayerIdRef
                    )
                );
            }
        }

        RSC absRsc;
        if (nodeNameSuffixedRscNamePair.objB.snapName == null)
        {
            absRsc = (RSC) parentObjects.rscMap.get(
                new Pair<>(
                    nodeNameSuffixedRscNamePair.objA,
                    nodeNameSuffixedRscNamePair.objB.rscName
                )
            );
        }
        else
        {
            absRsc = (RSC) parentObjects.snapMap.get(
                new Triple<>(
                    nodeNameSuffixedRscNamePair.objA,
                    nodeNameSuffixedRscNamePair.objB.rscName,
                    nodeNameSuffixedRscNamePair.objB.snapName
                )
            );
        }
        RscDataLoadOutput<RSC_DATA, VLM_DATA> loadedPair = loadImpl(
            rscDataRawCache.get(rscLayerIdRef),
            parentObjects,
            dummyLoadingRLO,
            nodeNameSuffixedRscNamePair,
            getParentRLO(dummyLoadingRLO, parentObjects),
            absRsc
        );

        RSC_DATA rscData = loadedPair.rscData;
        parentObjects.loadedLayerResourceObjectyById.put(rscData.getRscLayerId(), rscData);
        rscDataByLayerId.put(rscData.getRscLayerId(), loadedPair);

        if (rscDfnDriver != null)
        {
            RscDfnLayerObject rscDfnData = rscData.getRscDfnLayerObject();
            if (rscDfnData != null)
            {
                loadedRscDfnChildRscDataMap.get(rscDfnData).add(rscData);
            }
        }

        return new Pair<>(
            (AbsRscLayerObject<RSC>) loadedPair.rscData,
            (Set<AbsRscLayerObject<RSC>>) ((Object) loadedPair.rscDataChildren)
        );
    }

    protected AbsRscLayerObject<?> getDummyLoadingRLO(int rscLayerIdRef) throws DatabaseException
    {
        return getDummyLoadingRLO(rscLayerIdRef, true);
    }

    protected @Nullable AbsRscLayerObject<?> getDummyLoadingRLO(
        int rscLayerIdRef,
        boolean failIfNull
    )
        throws DatabaseException
    {
        AbsRscLayerObject<?> ret = parentObjects.dummyLayerResourceObjectyById.get(rscLayerIdRef);
        if (ret == null && failIfNull)
        {
            throw new DatabaseException(
                "Attempt to load unknown resource data: " + rscLayerIdRef + " for layer: " + getDeviceLayerKind()
            );
        }
        return ret;
    }

    protected abstract <RSC extends AbsResource<RSC>> RscDataLoadOutput<RSC_DATA, VLM_DATA> loadImpl(
        RawParameters rawRef,
        ParentObjects parentRef,
        AbsRscLayerObject<?> currentDummyLoadingRLORef,
        PairNonNull<NodeName, SuffixedResourceName> nodeNameSuffixedRscNamePairRef,
        @Nullable AbsRscLayerObject<?> loadedParentRscDataRef,
        RSC absRscRef
    )
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException,
        MdException;

    public LayerResourceIdDatabaseDriver getIdDriver()
    {
        return rscLayerIdDriver;
    }

    public static PairNonNull<NodeName, SuffixedResourceName> getNodeNameSuffixedRscNamePair(AbsRscLayerObject<?> rlo)
    {
        return new PairNonNull<>(
            rlo.getNodeName(),
            new SuffixedResourceName(
                rlo.getResourceName(),
                rlo.getSnapName(),
                rlo.getResourceNameSuffix()
            )
        );
    }

    @SuppressWarnings("unchecked")
    protected <RSC extends AbsResource<RSC>> @Nullable AbsRscLayerObject<RSC> getParentRLO(
        AbsRscLayerObject<?> dummyLoadingRLO,
        ParentObjects parentRef
    )
    {
        AbsRscLayerObject<RSC> ret = null;
        AbsRscLayerObject<?> dummyParent = dummyLoadingRLO.getParent();
        if (dummyParent != null)
        {
            ret = (AbsRscLayerObject<RSC>) parentRef.loadedLayerResourceObjectyById.get(
                dummyParent.getRscLayerId()
            );
        }
        return ret;
    }

    protected RSC_DFN_DATA getRscDfnData(SuffixedResourceName suffixedRscNameRef)
    {
        return allRscDfnData.get(suffixedRscNameRef);
    }

    protected VLM_DFN_DATA getVlmDfnData(SuffixedResourceName suffixedRscNameRef, VolumeNumber vlmNrRef)
    {
        return allVlmDfnData.get(new Pair<>(suffixedRscNameRef, vlmNrRef));
    }

    @Override
    public String getId(RSC_DATA rscDataRef)
    {
        return "(" + rscDataRef.getLayerKind().name() +
            ", LayerRscId=" + rscDataRef.getRscLayerId() + ")";
    }

    static class NullTableLayerRscDataColumn implements Column
    {
        protected static final String NULL_TABLE_LAYER_RSC_ID_OBJ = "NullTableLayerRscIdObj";

        @Override
        public String getName()
        {
            return NULL_TABLE_LAYER_RSC_ID_OBJ;
        }

        @Override
        public int getSqlType()
        {
            return java.sql.Types.OTHER;
        }

        @Override
        public boolean isPk()
        {
            return false;
        }

        @Override
        public boolean isNullable()
        {
            return false;
        }

        @Override
        public @Nullable DatabaseTable getTable()
        {
            return null;
        }
    }
}
