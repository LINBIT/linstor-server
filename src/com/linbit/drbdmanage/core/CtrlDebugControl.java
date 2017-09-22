package com.linbit.drbdmanage.core;

import com.linbit.drbdmanage.CommonDebugControl;
import com.linbit.drbdmanage.dbcp.DbConnectionPool;

public interface CtrlDebugControl extends CommonDebugControl
{
    Controller getModuleInstance();
    public DbConnectionPool getDbConnectionPool();
}