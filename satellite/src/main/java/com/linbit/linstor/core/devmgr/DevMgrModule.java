package com.linbit.linstor.core.devmgr;

import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.core.UpdateMonitor;
import com.linbit.linstor.core.UpdateMonitorImpl;
import com.linbit.linstor.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.Privilege;
import com.linbit.linstor.security.PrivilegeSet;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

public class DevMgrModule extends AbstractModule
{
    public static final String STLT_CONF_LOCK = "stltConfLock";
    public static final String DRBD_CONFIG_PATH = "DrbdConfigPath";

    private static final String DB_SATELLITE_PROPSCON_INSTANCE_NAME = "STLTCFG";

    @Override
    protected void configure()
    {
        bind(UpdateMonitor.class).to(UpdateMonitorImpl.class);
        // bind(DeviceManager.class).to(DeviceManagerImpl.class);
        // install(new FactoryModuleBuilder()
        //     .implement(DeviceManagerImpl.DeviceHandlerInvocation.class,
        //         DeviceManagerImpl.DeviceHandlerInvocation.class)
        //     .build(DeviceManagerImpl.DeviceHandlerInvocationFactory.class));
        bind(DeviceManager.class).to(DeviceManagerImpl.class);
        bind(NotificationListener.class).to(DeviceManagerImpl.class);
        bind(DeviceHandler.class).to(DeviceHandlerImpl.class);
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
        devMgrPriv.enablePrivileges(Privilege.PRIV_MAC_OVRD, Privilege.PRIV_OBJ_CONTROL);
        return devMgrCtx;
    }
}
