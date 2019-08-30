package com.linbit.linstor.core.objects;

import com.linbit.CollectionDatabaseDriver;
import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.ResourceDefinition.InitMaps;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseLoader;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDefinitionDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.Base64;
import com.linbit.utils.Pair;
import com.linbit.utils.StringUtils;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.ResourceDefinitions.LAYER_STACK;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.ResourceDefinitions.RESOURCE_DSP_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.ResourceDefinitions.RESOURCE_EXTERNAL_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.ResourceDefinitions.RESOURCE_FLAGS;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.ResourceDefinitions.RESOURCE_GROUP_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.ResourceDefinitions.RESOURCE_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.ResourceDefinitions.UUID;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Map;
import java.util.TreeMap;

@Singleton
public class ResourceDefinitionDbDriver
    extends
    AbsDatabaseDriver<ResourceDefinitionData, ResourceDefinition.InitMaps, Map<ResourceGroupName, ResourceGroup>>
    implements ResourceDefinitionDataDatabaseDriver
{
    private final AccessContext dbCtx;

    private final Provider<TransactionMgr> transMgrProvider;
    private final StateFlagsPersistence<ResourceDefinitionData> flagsDriver;
    private final CollectionDatabaseDriver<ResourceDefinitionData, DeviceLayerKind> layerStackDrvier;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;

    @Inject
    public ResourceDefinitionDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        Provider<TransactionMgr> transMgrProviderRef,
        ObjectProtectionDatabaseDriver objProtDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(errorReporterRef, GeneratedDatabaseTables.RESOURCE_DEFINITIONS, dbEngineRef, objProtDriverRef);
        dbCtx = dbCtxRef;
        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;

        setColumnSetter(UUID, rscDfn -> rscDfn.getUuid().toString());
        setColumnSetter(RESOURCE_NAME, rscDfn -> rscDfn.getName().value);
        setColumnSetter(RESOURCE_DSP_NAME, rscDfn -> rscDfn.getName().displayValue);
        setColumnSetter(RESOURCE_FLAGS, rscDfn -> rscDfn.getFlags().getFlagsBits(dbCtx));
        setColumnSetter(
            LAYER_STACK, rscDfn -> toString(StringUtils.asStrList(rscDfn.getLayerStack(dbCtxRef)))
        );
        setColumnSetter(RESOURCE_GROUP_NAME, rscDfn -> rscDfn.getResourceGroup().getName().value);
        switch (getDbType())
        {
            case ETCD:
                setColumnSetter(
                    RESOURCE_EXTERNAL_NAME,
                    rscDfn -> {
                        byte[] extName = rscDfn.getExternalName();
                        String extNameBase64 = null;
                        if (extName != null)
                        {
                            extNameBase64 = Base64.encode(rscDfn.getExternalName());
                        }
                        return extNameBase64;
                    }
                );
                break;
            case SQL:
                setColumnSetter(RESOURCE_EXTERNAL_NAME, ResourceDefinition::getExternalName);
                break;
            default:
                throw new ImplementationError("Unknown database type: " + getDbType());
        }


        flagsDriver = generateFlagDriver(RESOURCE_FLAGS, ResourceDefinition.RscDfnFlags.class);
        layerStackDrvier = generateCollectionToJsonStringArrayDriver(LAYER_STACK);
    }

    @Override
    public StateFlagsPersistence<ResourceDefinitionData> getStateFlagsPersistence()
    {
        return flagsDriver;
    }

    @Override
    public CollectionDatabaseDriver<ResourceDefinitionData, DeviceLayerKind> getLayerStackDriver()
    {
        return layerStackDrvier;
    }

    @Override
    protected Pair<ResourceDefinitionData, InitMaps> load(
        RawParameters raw,
        Map<ResourceGroupName, ResourceGroup> rscGrpMap
    )
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException
    {
        final Map<VolumeNumber, VolumeDefinition> vlmDfnMap = new TreeMap<>();
        final Map<NodeName, Resource> rscMap = new TreeMap<>();
        final Map<SnapshotName, SnapshotDefinition> snapshotDfnMap = new TreeMap<>();
        final ResourceName rscName = raw.build(RESOURCE_DSP_NAME, ResourceName::new);
        final long flags;
        final byte[] extName;

        switch (getDbType())
        {
            case ETCD:
                flags = Long.parseLong(raw.get(RESOURCE_FLAGS));
                String extNameBase64 = raw.get(RESOURCE_EXTERNAL_NAME);
                extName = extNameBase64 != null ? Base64.decode(extNameBase64) : null;
                break;
            case SQL:
                flags = raw.get(RESOURCE_FLAGS);
                extName = raw.get(RESOURCE_EXTERNAL_NAME);
                break;
            default:
                throw new ImplementationError("Unknown database type: " + getDbType());
        }
        return new Pair<>(
            new ResourceDefinitionData(
                raw.build(UUID, java.util.UUID::fromString),
                getObjectProtection(ObjectProtection.buildPath(rscName)),
                rscName,
                extName,
                flags,
                DatabaseLoader.asDevLayerKindList(raw.getAsStringList(LAYER_STACK)),
                this,
                propsContainerFactory,
                transObjFactory,
                transMgrProvider,
                vlmDfnMap,
                rscMap,
                snapshotDfnMap,
                new TreeMap<>(),
                rscGrpMap.get(raw.build(RESOURCE_GROUP_NAME, ResourceGroupName::new))
            ),
            new RscDfnInitMaps(vlmDfnMap, rscMap, snapshotDfnMap)
        );

    }

    @Override
    protected String getId(ResourceDefinitionData rscDfn)
    {
        return "(ResName=" + rscDfn.getName().displayValue + ")";
    }

    class RscDfnInitMaps implements ResourceDefinition.InitMaps
    {
        private final Map<VolumeNumber, VolumeDefinition> vlmDfnMap;
        private final Map<NodeName, Resource> rscMap;
        private final Map<SnapshotName, SnapshotDefinition> snapshotDfnMap;

        RscDfnInitMaps(
            Map<VolumeNumber, VolumeDefinition> vlmDfnMapRef,
            Map<NodeName, Resource> rscMapRef,
            Map<SnapshotName, SnapshotDefinition> snapshotDfnMapRef
        )
        {
            this.vlmDfnMap = vlmDfnMapRef;
            this.rscMap = rscMapRef;
            this.snapshotDfnMap = snapshotDfnMapRef;
        }

        @Override
        public Map<NodeName, Resource> getRscMap()
        {
            return rscMap;
        }

        @Override
        public Map<VolumeNumber, VolumeDefinition> getVlmDfnMap()
        {
            return vlmDfnMap;
        }

        @Override
        public Map<SnapshotName, SnapshotDefinition> getSnapshotDfnMap()
        {
            return snapshotDfnMap;
        }
    }

}
