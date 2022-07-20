package com.linbit.linstor.dbdrivers.interfaces.remotes;

import com.linbit.linstor.core.objects.remotes.EbsRemote;
import com.linbit.linstor.dbdrivers.ControllerDatabaseDriver;

public interface EbsRemoteCtrlDatabaseDriver
    extends EbsRemoteDatabaseDriver, ControllerDatabaseDriver<EbsRemote, EbsRemote.InitMaps, Void>
{

}
