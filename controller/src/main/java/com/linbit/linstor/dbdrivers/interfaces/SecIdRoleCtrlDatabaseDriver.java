package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.dbdrivers.ControllerDatabaseDriver;
import com.linbit.linstor.security.Identity;
import com.linbit.linstor.security.Role;
import com.linbit.utils.Pair;

public interface SecIdRoleCtrlDatabaseDriver extends SecIdRoleDatabaseDriver,
    ControllerDatabaseDriver<Pair<Identity, Role>, Void, Void>
{
}
