package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.dbdrivers.ControllerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecConfigDatabaseDriver.SecConfigDbEntry;

public interface SecConfigCtrlDatabaseDriver extends SecConfigDatabaseDriver,
    ControllerDatabaseDriver<SecConfigDbEntry, Void, Void>
{

}
