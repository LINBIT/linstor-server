package com.linbit.linstor.core.devmgr;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.core.UpdateMonitor;
import com.linbit.linstor.core.UpdateMonitorImpl;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.Privilege;
import com.linbit.linstor.security.PrivilegeSet;
import com.linbit.linstor.storage.layer.DeviceLayer.NotificationListener;

import javax.inject.Singleton;

public class DevMgrModule extends AbstractModule
{
    public static final String LOCAL_NODE_PROPS = "localNodeProps";

    public static final String STLT_CONF_LOCK = "stltConfLock";
    public static final String DRBD_CONFIG_PATH = "DrbdConfigPath";

    private static final String DB_SATELLITE_PROPSCON_INSTANCE_NAME = "STLTCFG";

    // Path to the DRBD configuration files; this should be replaced by some meaningful constant or possibly
    // a value configurable in the cluster configuration
    public static final String CONFIG_PATH = "/var/lib/linstor.d";

    @Override
    protected void configure()
    {
        bind(UpdateMonitor.class).to(UpdateMonitorImpl.class);
        // bind(DeviceManager.class).to(DeviceManagerImpl.class);
        // install(new FactoryModuleBuilder()
        //     .implement(DeviceManagerImpl.DeviceHandlerInvocation.class,
        //         DeviceManagerImpl.DeviceHandlerInvocation.class)
        //     .build(DeviceManagerImpl.DeviceHandlerInvocationFactory.class));
        bind(DeviceManager.class).to(DeviceManagerImpl2.class);
        bind(NotificationListener.class).to(DeviceManagerImpl2.class);
        bind(DeviceHandler2.class).to(DeviceHandlerImpl.class);
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
