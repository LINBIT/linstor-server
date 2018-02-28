package com.linbit.linstor.security;

import com.linbit.InvalidNameException;
import com.linbit.linstor.ControllerDatabase;

import java.sql.SQLException;

/**
 * Initializes security objects.
 */
public final class Initializer
{
    public static void load(AccessContext accCtx, ControllerDatabase ctrlDb, DbAccessor driver)
        throws SQLException, AccessDeniedException, InvalidNameException
    {
        accCtx.getEffectivePrivs().requirePrivileges(Privilege.PRIV_SYS_ALL);

        SecurityLevel.load(ctrlDb, driver);
        Identity.load(ctrlDb, driver);
        SecurityType.load(ctrlDb, driver);
        Role.load(ctrlDb, driver);
    }

    private Initializer()
    {
    }
}
