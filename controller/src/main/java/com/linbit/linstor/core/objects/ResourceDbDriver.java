package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource.InitMaps;
import com.linbit.linstor.dbdrivers.AbsProtectedDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.ResourceCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.utils.Pair;
import com.linbit.utils.PairNonNull;

import static com.linbit.linstor.core.objects.ResourceDefinitionDbDriver.DFLT_SNAP_NAME_FOR_RSC;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Resources.CREATE_TIMESTAMP;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Resources.NODE_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Resources.RESOURCE_FLAGS;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Resources.RESOURCE_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Resources.SNAPSHOT_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Resources.UUID;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

@Singleton
public final class ResourceDbDriver extends
    AbsProtectedDatabaseDriver<AbsResource<Resource>, Resource.InitMaps, PairNonNull<Map<NodeName, Node>,
    Map<ResourceName, ResourceDefinition>>>
    implements ResourceCtrlDatabaseDriver
{

    private final StateFlagsPersistence<AbsResource<Resource>> flagsDriver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    protected final SingleColumnDatabaseDriver<AbsResource<Resource>, Date> createTimestampDriver;

    @Inject
    public ResourceDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngine,
        Provider<TransactionMgr> transMgrProviderRef,
        ObjectProtectionFactory objProtFactoryRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(
            dbCtxRef,
            errorReporterRef,
            GeneratedDatabaseTables.RESOURCES,
            dbEngine,
            objProtFactoryRef
        );
        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;

        flagsDriver = generateFlagDriver(RESOURCE_FLAGS, Resource.Flags.class);

        Function<Date, Object> createTimestampTypeMapper;

        setColumnSetter(UUID, rsc -> rsc.getUuid().toString());
        setColumnSetter(NODE_NAME, rsc -> rsc.getNode().getName().value);
        setColumnSetter(RESOURCE_NAME, rsc -> rsc.getResourceDefinition().getName().value);
        setColumnSetter(RESOURCE_FLAGS, rsc -> ((Resource) rsc).getStateFlags().getFlagsBits(dbCtxRef));
        setColumnSetter(SNAPSHOT_NAME, ignored -> DFLT_SNAP_NAME_FOR_RSC);
        switch (getDbType())
        {
            case SQL:
                setColumnSetter(CREATE_TIMESTAMP, rsc -> rsc.getCreateTimestamp().isPresent() ?
                    new Timestamp(rsc.getCreateTimestamp().get().getTime()) : null);
                createTimestampTypeMapper = createTime -> createTime != null ?
                    new Timestamp(createTime.getTime()) :
                    null;
                break;
            case K8S_CRD:
                setColumnSetter(CREATE_TIMESTAMP, rsc -> rsc.getCreateTimestamp().isPresent() ?
                    rsc.getCreateTimestamp().get().getTime() : null);
                createTimestampTypeMapper = createTime -> createTime != null ? createTime.getTime() : null;
                break;
            default:
                throw new ImplementationError("Unknown database type: " + getDbType());
        }

        createTimestampDriver = generateSingleColumnDriver(
            CREATE_TIMESTAMP,
            rsc -> rsc.getCreateTimestamp().isPresent() ? rsc.getCreateTimestamp().get().toString() : "null",
            createTimestampTypeMapper
        );
    }

    @Override
    public StateFlagsPersistence<AbsResource<Resource>> getStateFlagPersistence()
    {
        return flagsDriver;
    }

    @Override
    protected @Nullable Pair<AbsResource<Resource>, Resource.InitMaps> load(
        RawParameters raw,
        PairNonNull<Map<NodeName, Node>, Map<ResourceName, ResourceDefinition>> loadAllDataRef
    )
        throws DatabaseException, InvalidNameException, InvalidIpAddressException, ValueOutOfRangeException
    {
        final Pair<AbsResource<Resource>, InitMaps> ret;
        if (!raw.get(SNAPSHOT_NAME).equals(DFLT_SNAP_NAME_FOR_RSC))
        {
            // this entry is a Snapshot, not a Resource
            ret = null;
        }
        else
        {
            NodeName nodeName = new NodeName(raw.get(NODE_NAME));
            ResourceName rscName = new ResourceName(raw.get(RESOURCE_NAME));
            Long createTimestamp = null;
            Map<Resource.ResourceKey, ResourceConnection> rscConMap = new TreeMap<>();
            Map<VolumeNumber, Volume> vlmMap = new TreeMap<>();

            final long flags;
            switch (getDbType())
            {
                case SQL:
                case K8S_CRD:
                    flags = raw.get(RESOURCE_FLAGS);
                    createTimestamp = raw.get(CREATE_TIMESTAMP);
                    break;
                default:
                    throw new ImplementationError("Unknown database type: " + getDbType());
            }

            ret = new Pair<>(
                new Resource(
                    raw.build(UUID, java.util.UUID::fromString),
                    getObjectProtection(ObjectProtection.buildPath(nodeName, rscName)),
                    loadAllDataRef.objB.get(rscName),
                    loadAllDataRef.objA.get(nodeName),
                    flags,
                    this,
                    propsContainerFactory,
                    transObjFactory,
                    transMgrProvider,
                    rscConMap,
                    vlmMap,
                    createTimestamp == null ? null : new Date(createTimestamp)
                ),
                new InitMapImpl(rscConMap, vlmMap)
            );
        }
        return ret;
    }

    @Override
    protected String getId(AbsResource<Resource> rsc)
    {
        return "(NodeName=" + rsc.getNode().getName().displayValue +
            " ResName=" + rsc.getResourceDefinition().getName().displayValue + ")";
    }

    public static class InitMapImpl implements Resource.InitMaps
    {
        private final Map<Resource.ResourceKey, ResourceConnection> rscConMap;
        private final Map<VolumeNumber, Volume> vlmMap;

        public InitMapImpl(
            Map<Resource.ResourceKey, ResourceConnection> rscConMapRef,
            Map<VolumeNumber, Volume> vlmMapRef
        )
        {
            rscConMap = rscConMapRef;
            vlmMap = vlmMapRef;
        }

        @Override
        public Map<Resource.ResourceKey, ResourceConnection> getRscConnMap()
        {
            return rscConMap;
        }

        @Override
        public Map<VolumeNumber, Volume> getVlmMap()
        {
            return vlmMap;
        }
    }

    @Override
    public SingleColumnDatabaseDriver<AbsResource<Resource>, Date> getCreateTimeDriver()
    {
        return createTimestampDriver;
    }
}
