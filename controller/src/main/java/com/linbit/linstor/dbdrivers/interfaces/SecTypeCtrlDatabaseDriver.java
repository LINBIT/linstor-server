package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.dbdrivers.ControllerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecTypeCtrlDatabaseDriver.SecTypeInitObj;
import com.linbit.linstor.security.SecurityType;

public interface SecTypeCtrlDatabaseDriver extends SecTypeDatabaseDriver,
    ControllerDatabaseDriver<SecurityType, SecTypeInitObj, Void>
{
    class SecTypeInitObj
    {
        private final boolean enabled;

        public SecTypeInitObj(boolean enabledRef)
        {
            enabled = enabledRef;
        }

        boolean isEnabled()
        {
            return enabled;
        }
    }
}
