package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.LinstorRemote;
import com.linbit.linstor.dbdrivers.ControllerDatabaseDriver;

public interface LinstorRemoteCtrlDatabaseDriver
    extends LinstorRemoteDatabaseDriver, ControllerDatabaseDriver<LinstorRemote, LinstorRemote.InitMaps, Void>
{

}
