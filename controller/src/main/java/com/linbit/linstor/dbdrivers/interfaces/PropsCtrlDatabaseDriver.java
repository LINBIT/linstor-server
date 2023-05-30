package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.dbdrivers.ControllerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.PropsDatabaseDriver.PropsDbEntry;

public interface PropsCtrlDatabaseDriver extends PropsDatabaseDriver,
    ControllerDatabaseDriver<PropsDbEntry, Void, Void>
{
    void clearCache();
}
