package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.ExternalFile;
import com.linbit.linstor.dbdrivers.ControllerDatabaseDriver;

public interface ExternalFileCtrlDatabaseDriver extends ExternalFileDatabaseDriver,
    ControllerDatabaseDriver<ExternalFile, ExternalFile.InitMaps, Void>
{

}
