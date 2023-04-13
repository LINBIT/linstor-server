package com.linbit.linstor.dbdrivers;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.linstor.core.objects.BCacheLayerSQLDbDriver;
import com.linbit.linstor.core.objects.CacheLayerSQLDbDriver;
import com.linbit.linstor.core.objects.LayerDrbdRscDbDriver;
import com.linbit.linstor.core.objects.LayerDrbdRscDfnDbDriver;
import com.linbit.linstor.core.objects.LayerDrbdVlmDbDriver;
import com.linbit.linstor.core.objects.LayerDrbdVlmDfnDbDriver;
import com.linbit.linstor.core.objects.LayerLuksRscDbDriver;
import com.linbit.linstor.core.objects.LayerLuksVlmDbDriver;
import com.linbit.linstor.core.objects.LayerNvmeRscDbDriver;
import com.linbit.linstor.core.objects.LayerResourceIdDbDriver;
import com.linbit.linstor.core.objects.LayerStorageRscDbDriver;
import com.linbit.linstor.core.objects.LayerStorageVlmDbDriver;
import com.linbit.linstor.core.objects.NetInterfaceDbDriver;
import com.linbit.linstor.core.objects.NodeConnectionDbDriver;
import com.linbit.linstor.core.objects.NodeGenericDbDriver;
import com.linbit.linstor.core.objects.OpenflexLayerSQLDbDriver;
import com.linbit.linstor.core.objects.ResourceConnectionDbDriver;
import com.linbit.linstor.core.objects.ResourceDbDriver;
import com.linbit.linstor.core.objects.ResourceDefinitionDbDriver;
import com.linbit.linstor.core.objects.ResourceGroupDbDriver;
import com.linbit.linstor.core.objects.SnapshotDbDriver;
import com.linbit.linstor.core.objects.SnapshotDefinitionDbDriver;
import com.linbit.linstor.core.objects.SnapshotVolumeDbDriver;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinitionDbDriver;
import com.linbit.linstor.core.objects.StorPoolDbDriver;
import com.linbit.linstor.core.objects.StorPoolDefinitionDbDriver;
import com.linbit.linstor.core.objects.VolumeConnectionDbDriver;
import com.linbit.linstor.core.objects.VolumeDbDriver;
import com.linbit.linstor.core.objects.VolumeDefinitionDbDriver;
import com.linbit.linstor.core.objects.WritecacheLayerSQLDbDriver;
import com.linbit.linstor.dbdrivers.interfaces.BCacheLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.CacheLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerDrbdRscCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerDrbdRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerDrbdRscDfnDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerDrbdVlmDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerDrbdVlmDfnDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerLuksRscCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerLuksRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerLuksVlmDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerNvmeRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerStorageRscCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerStorageRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerStorageVlmDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NetInterfaceDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NodeConnectionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NodeDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.OpenflexLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.PropsConDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceConnectionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDefinitionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceGroupDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDefinitionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDefinitionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDefinitionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeConnectionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDefinitionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.WritecacheLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.sql.SQLEngine;
import com.linbit.linstor.propscon.PropsConSQLDbDriver;
import com.linbit.linstor.security.DbAccessor;
import com.linbit.linstor.security.DbSQLPersistence;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.linstor.security.ObjectProtectionSQLDbDriver;

import com.google.inject.AbstractModule;

public class TestDbModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(DbAccessor.class).to(DbSQLPersistence.class);
        bind(DbEngine.class).to(SQLEngine.class);

        bind(ObjectProtectionDatabaseDriver.class).to(ObjectProtectionSQLDbDriver.class);

        // bind(DatabaseDriver.class).to(DatabaseLoader.class);
        bind(DatabaseDriver.class).toInstance(new DatabaseDriver()
        {

            @Override
            public void loadAll() throws DatabaseException
            {
                // noop
            }

            @Override
            public ServiceName getDefaultServiceInstanceName()
            {
                try
                {
                    return new ServiceName("NoopTestDatabaseDriver");
                }
                catch (InvalidNameException exc)
                {
                    throw new ImplementationError(exc);
                }
            }
        });

        bind(PropsConDatabaseDriver.class).to(PropsConSQLDbDriver.class);
        bind(NodeDatabaseDriver.class).to(NodeGenericDbDriver.class);
        bind(ResourceGroupDatabaseDriver.class).to(ResourceGroupDbDriver.class);
        bind(ResourceDefinitionDatabaseDriver.class).to(ResourceDefinitionDbDriver.class);
        bind(ResourceDatabaseDriver.class).to(ResourceDbDriver.class);
        bind(VolumeDefinitionDatabaseDriver.class).to(VolumeDefinitionDbDriver.class);
        bind(VolumeDatabaseDriver.class).to(VolumeDbDriver.class);
        bind(StorPoolDefinitionDatabaseDriver.class).to(StorPoolDefinitionDbDriver.class);
        bind(StorPoolDatabaseDriver.class).to(StorPoolDbDriver.class);
        bind(NetInterfaceDatabaseDriver.class).to(NetInterfaceDbDriver.class);
        bind(NodeConnectionDatabaseDriver.class).to(NodeConnectionDbDriver.class);
        bind(ResourceConnectionDatabaseDriver.class).to(ResourceConnectionDbDriver.class);
        bind(VolumeConnectionDatabaseDriver.class).to(VolumeConnectionDbDriver.class);
        bind(SnapshotDefinitionDatabaseDriver.class).to(SnapshotDefinitionDbDriver.class);
        bind(SnapshotVolumeDefinitionDatabaseDriver.class).to(SnapshotVolumeDefinitionDbDriver.class);
        bind(SnapshotDatabaseDriver.class).to(SnapshotDbDriver.class);
        bind(SnapshotVolumeDatabaseDriver.class).to(SnapshotVolumeDbDriver.class);

        bind(LayerResourceIdDatabaseDriver.class).to(LayerResourceIdDbDriver.class);

        bind(LayerDrbdRscDfnDatabaseDriver.class).to(LayerDrbdRscDfnDbDriver.class);
        bind(LayerDrbdVlmDfnDatabaseDriver.class).to(LayerDrbdVlmDfnDbDriver.class);
        bind(LayerDrbdRscDatabaseDriver.class).to(LayerDrbdRscDbDriver.class);
        bind(LayerDrbdRscCtrlDatabaseDriver.class).to(LayerDrbdRscDbDriver.class);
        bind(LayerDrbdVlmDatabaseDriver.class).to(LayerDrbdVlmDbDriver.class);

        bind(LayerLuksRscCtrlDatabaseDriver.class).to(LayerLuksRscDbDriver.class);
        bind(LayerLuksRscDatabaseDriver.class).to(LayerLuksRscDbDriver.class);
        bind(LayerLuksVlmDatabaseDriver.class).to(LayerLuksVlmDbDriver.class);

        bind(LayerStorageRscDatabaseDriver.class).to(LayerStorageRscDbDriver.class);
        bind(LayerStorageRscCtrlDatabaseDriver.class).to(LayerStorageRscDbDriver.class);
        bind(LayerStorageVlmDatabaseDriver.class).to(LayerStorageVlmDbDriver.class);

        bind(LayerNvmeRscDatabaseDriver.class).to(LayerNvmeRscDbDriver.class);

        bind(OpenflexLayerDatabaseDriver.class).to(OpenflexLayerSQLDbDriver.class);
        bind(WritecacheLayerDatabaseDriver.class).to(WritecacheLayerSQLDbDriver.class);
        bind(CacheLayerDatabaseDriver.class).to(CacheLayerSQLDbDriver.class);
        bind(BCacheLayerDatabaseDriver.class).to(BCacheLayerSQLDbDriver.class);
    }
}
