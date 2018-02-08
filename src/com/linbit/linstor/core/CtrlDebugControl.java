package com.linbit.linstor.core;

import com.linbit.linstor.CommonDebugControl;
import com.linbit.linstor.dbcp.DbConnectionPool;

public interface CtrlDebugControl extends CommonDebugControl
{
    Controller getModuleInstance();
    DbConnectionPool getDbConnectionPool();
}
