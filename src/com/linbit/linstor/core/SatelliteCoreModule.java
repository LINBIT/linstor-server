package com.linbit.linstor.core;

import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;
import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.annotation.Uninitialized;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.Privilege;
import com.linbit.linstor.security.PrivilegeSet;
import com.linbit.linstor.transaction.SatelliteTransactionMgr;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Named;
import javax.inject.Singleton;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SatelliteCoreModule extends AbstractModule
{
    public static final String SATELLITE_PROPS = "SatelliteProps";

    public static final String STLT_CONF_LOCK = "stltConfLock";
    public static final String DRBD_CONFIG_PATH = "DrbdConfigPath";

    private static final String DB_SATELLITE_PROPSCON_INSTANCE_NAME = "STLTCFG";

    // Path to the DRBD configuration files; this should be replaced by some meaningful constant or possibly
    // a value configurable in the cluster configuration
    public static final String CONFIG_PATH = "/var/lib/drbd.d";

    @Override
    protected void configure()
    {
        bind(String.class).annotatedWith(Names.named(CoreModule.MODULE_NAME))
            .toInstance(Satellite.MODULE);

        // Core maps do not require initialization on the satellite
        bind(CoreModule.NodesMap.class)
            .to(Key.get(CoreModule.NodesMap.class, Uninitialized.class));
        bind(CoreModule.ResourceDefinitionMap.class)
            .to(Key.get(CoreModule.ResourceDefinitionMap.class, Uninitialized.class));
        bind(CoreModule.StorPoolDefinitionMap.class)
            .to(Key.get(CoreModule.StorPoolDefinitionMap.class, Uninitialized.class));

        bind(ReadWriteLock.class).annotatedWith(Names.named(STLT_CONF_LOCK))
            .toInstance(new ReentrantReadWriteLock(true));

        bind(ControllerPeerConnector.class).to(ControllerPeerConnectorImpl.class);
        bind(UpdateMonitor.class).to(UpdateMonitorImpl.class);
        bind(DeviceManager.class).to(DeviceManagerImpl.class);
        install(new FactoryModuleBuilder()
            .implement(DeviceManagerImpl.DeviceHandlerInvocation.class,
                DeviceManagerImpl.DeviceHandlerInvocation.class)
            .build(DeviceManagerImpl.DeviceHandlerInvocationFactory.class));

        bind(Path.class).annotatedWith(Names.named(DRBD_CONFIG_PATH)).toInstance(
            FileSystems.getDefault().getPath(CONFIG_PATH)
        );
    }

    @Provides
    @Singleton
    @Named(SATELLITE_PROPS)
    public Props loadPropsContainer(
        PropsContainerFactory propsContainerFactory,
        LinStorScope initScope
    )
    {
        Props propsContainer;
        try
        {
            SatelliteTransactionMgr transMgr = new SatelliteTransactionMgr();
            initScope.enter();
            initScope.seed(TransactionMgr.class, transMgr);

            propsContainer = propsContainerFactory.getInstance(DB_SATELLITE_PROPSCON_INSTANCE_NAME);

            transMgr.commit();
            initScope.exit();
        }
        catch (SQLException exc)
        {
            // not possible
            throw new ImplementationError(exc);
        }
        return propsContainer;
    }

    @Provides
    @Singleton
    @DeviceManagerContext
    public AccessContext deviceManagerContext(@SystemContext AccessContext systemCtx)
        throws AccessDeniedException
    {
        AccessContext devMgrCtx = systemCtx.clone();
        PrivilegeSet devMgrPriv = devMgrCtx.getEffectivePrivs();
        devMgrPriv.disablePrivileges(Privilege.PRIV_SYS_ALL);
        devMgrPriv.enablePrivileges(Privilege.PRIV_MAC_OVRD, Privilege.PRIV_OBJ_USE);
        return devMgrCtx;
    }
}
