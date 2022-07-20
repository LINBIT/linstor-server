package com.linbit.linstor.dbdrivers.interfaces.remotes;

import com.linbit.linstor.core.objects.remotes.LinstorRemote;
import com.linbit.linstor.dbdrivers.ControllerDatabaseDriver;

public interface LinstorRemoteCtrlDatabaseDriver
    extends LinstorRemoteDatabaseDriver, ControllerDatabaseDriver<LinstorRemote, LinstorRemote.InitMaps, Void>
{

}
