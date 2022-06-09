package com.linbit.linstor.core;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SatelliteCoreModule extends AbstractModule
{
    public static final String STLT_CONF_LOCK = "stltConfLock";
    public static final String DRBD_CONFIG_PATH = "DrbdConfigPath";

    @Override
    protected void configure()
    {
        bind(String.class).annotatedWith(Names.named(CoreModule.MODULE_NAME))
            .toInstance(LinStor.SATELLITE_MODULE);

        bind(ReadWriteLock.class).annotatedWith(Names.named(STLT_CONF_LOCK))
            .toInstance(new ReentrantReadWriteLock(true));

        bind(ControllerPeerConnector.class).to(ControllerPeerConnectorImpl.class);
        // bind(UpdateMonitor.class).to(UpdateMonitorImpl.class);
        // bind(DeviceManager.class).to(DeviceManagerImpl.class);
        // install(new FactoryModuleBuilder()
        //    .implement(DeviceManagerImpl.DeviceHandlerInvocation.class,
        //       DeviceManagerImpl.DeviceHandlerInvocation.class)
        //    .build(DeviceManagerImpl.DeviceHandlerInvocationFactory.class));

        bind(Path.class).annotatedWith(Names.named(DRBD_CONFIG_PATH)).toInstance(
            FileSystems.getDefault().getPath(LinStor.CONFIG_PATH)
        );
    }

    //    @Provides
    //    @Singleton
    //    @DeviceManagerContext
    //    public AccessContext deviceManagerContext(@SystemContext AccessContext systemCtx)
    //        throws AccessDeniedException
    //    {
    //        AccessContext devMgrCtx = systemCtx.clone();
    //        PrivilegeSet devMgrPriv = devMgrCtx.getEffectivePrivs();
    //        devMgrPriv.disablePrivileges(Privilege.PRIV_SYS_ALL);
    //        devMgrPriv.enablePrivileges(Privilege.PRIV_MAC_OVRD, Privilege.PRIV_OBJ_USE);
    //        return devMgrCtx;
    //    }
}
