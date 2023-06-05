package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.dbdrivers.ControllerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecIdentityDatabaseDriver.SecIdentityDbObj;

public interface SecIdentityCtrlDatabaseDriver extends SecIdentityDatabaseDriver,
    ControllerDatabaseDriver<SecIdentityDbObj, Void, Void>
{
}
