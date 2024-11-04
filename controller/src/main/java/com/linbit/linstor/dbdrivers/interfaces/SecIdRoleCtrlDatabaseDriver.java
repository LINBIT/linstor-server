package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.dbdrivers.ControllerDatabaseDriver;
import com.linbit.linstor.security.Identity;
import com.linbit.linstor.security.Role;
import com.linbit.utils.PairNonNull;

public interface SecIdRoleCtrlDatabaseDriver extends SecIdRoleDatabaseDriver,
    ControllerDatabaseDriver<PairNonNull<Identity, Role>, Void, Void>
{
}
