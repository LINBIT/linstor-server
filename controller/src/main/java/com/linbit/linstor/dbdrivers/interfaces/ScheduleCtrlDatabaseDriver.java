package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.Schedule;
import com.linbit.linstor.dbdrivers.ControllerDatabaseDriver;


public interface ScheduleCtrlDatabaseDriver
    extends ScheduleDatabaseDriver, ControllerDatabaseDriver<Schedule, Schedule.InitMaps, Void>
{

}
