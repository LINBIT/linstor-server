package com.linbit.linstor.security;

import com.linbit.InvalidNameException;
import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.dbdrivers.DatabaseException;

/**
 * Initializes security objects.
 */
public final class Initializer
{
    public static void load(AccessContext accCtx, ControllerDatabase ctrlDb, DbAccessor driver)
        throws DatabaseException, AccessDeniedException, InvalidNameException
    {
        accCtx.getEffectivePrivs().requirePrivileges(Privilege.PRIV_SYS_ALL);

        SecurityLevel.load(ctrlDb, driver);
        Authentication.load(ctrlDb, driver);
        Identity.load(ctrlDb, driver);
        SecurityType.load(ctrlDb, driver);
        Role.load(ctrlDb, driver);
    }

    private Initializer()
    {
    }
}
