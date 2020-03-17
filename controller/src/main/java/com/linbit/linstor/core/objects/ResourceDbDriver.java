package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource.InitMaps;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.interfaces.ResourceCtrlDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.utils.Pair;

import static com.linbit.linstor.core.objects.ResourceDefinitionDbDriver.DFLT_SNAP_NAME_FOR_RSC;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Resources.NODE_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Resources.RESOURCE_FLAGS;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Resources.RESOURCE_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Resources.SNAPSHOT_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Resources.UUID;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Map;
import java.util.TreeMap;

@Singleton
public class ResourceDbDriver extends
    AbsDatabaseDriver<Resource, Resource.InitMaps, Pair<Map<NodeName, Node>, Map<ResourceName, ResourceDefinition>>>
    implements ResourceCtrlDatabaseDriver
{

    private final StateFlagsPersistence<Resource> flagsDriver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public ResourceDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngine,
        Provider<TransactionMgr> transMgrProviderRef,
        ObjectProtectionDatabaseDriver objProtDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(
            errorReporterRef,
            GeneratedDatabaseTables.RESOURCES,
            dbEngine,
            objProtDriverRef
        );
        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;

        flagsDriver = generateFlagDriver(RESOURCE_FLAGS, Resource.Flags.class);

        setColumnSetter(UUID, rsc -> rsc.getUuid().toString());
        setColumnSetter(NODE_NAME, rsc -> rsc.getNode().getName().value);
        setColumnSetter(RESOURCE_NAME, rsc -> rsc.getDefinition().getName().value);
        setColumnSetter(RESOURCE_FLAGS, rsc -> rsc.getStateFlags().getFlagsBits(dbCtxRef));
        setColumnSetter(SNAPSHOT_NAME, ignored -> DFLT_SNAP_NAME_FOR_RSC);
    }

    @Override
    public StateFlagsPersistence<Resource> getStateFlagPersistence()
    {
        return flagsDriver;
    }

    @Override
    protected Pair<Resource, Resource.InitMaps> load(
        RawParameters raw,
        Pair<Map<NodeName, Node>, Map<ResourceName, ResourceDefinition>> loadAllDataRef
    )
        throws DatabaseException, InvalidNameException, InvalidIpAddressException, ValueOutOfRangeException
    {
        final Pair<Resource, InitMaps> ret;
        if (!raw.get(SNAPSHOT_NAME).equals(DFLT_SNAP_NAME_FOR_RSC))
        {
            // this entry is a Snapshot, not a Resource
            ret = null;
        }
        else
        {
            NodeName nodeName = new NodeName(raw.get(NODE_NAME));
            ResourceName rscName = new ResourceName(raw.get(RESOURCE_NAME));
            Map<Resource.ResourceKey, ResourceConnection> rscConMap = new TreeMap<>();
            Map<VolumeNumber, Volume> vlmMap = new TreeMap<>();

            final long flags;
            switch (getDbType())
            {
                case ETCD:
                    flags = Long.parseLong(raw.get(RESOURCE_FLAGS));
                    break;
                case SQL:
                    flags = raw.get(RESOURCE_FLAGS);
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
                    vlmMap
                ),
                new InitMapImpl(rscConMap, vlmMap)
            );
        }
        return ret;
    }

    @Override
    protected String getId(Resource rsc)
    {
        return "(NodeName=" + rsc.getNode().getName().displayValue +
            " ResName=" + rsc.getDefinition().getName().displayValue + ")";
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
}
