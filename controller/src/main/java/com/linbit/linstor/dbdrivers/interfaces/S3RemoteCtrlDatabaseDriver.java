package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.S3Remote;
import com.linbit.linstor.dbdrivers.ControllerDatabaseDriver;

public interface S3RemoteCtrlDatabaseDriver
    extends S3RemoteDatabaseDriver, ControllerDatabaseDriver<S3Remote, S3Remote.InitMaps, Void>
{

}
