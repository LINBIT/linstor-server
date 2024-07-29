package com.linbit.linstor.security;

import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.DatabaseException;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Global security level used for linstor object protection
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public enum SecurityLevel
{
    // No security - Object protection off
    NO_SECURITY,

    // Role based access control - Objects protected by access control lists
    RBAC,

    // Mandatory access control - Objects additionaly protected by domain/type rules
    MAC;

    private static final AtomicReference<SecurityLevel> GLOBAL_SEC_LEVEL_REF =
        new AtomicReference<>(SecurityLevel.MAC);

    public static SecurityLevel get()
    {
        return GLOBAL_SEC_LEVEL_REF.get();
    }

    /**
     * Sets a new global security level
     *
     * All other security-related operations must be locked out when changing the security level.
     * (Caller must hold the Controller's or Satellite's reconfiguration lock in write mode)
     *
     * A security level change is always performed as an isolated action.
     * (not part of another transaction)
     *
     * @param accCtx An access context authorized to change the security level
     * @param newLevel The new security level to set
     * @param ctrlDb Reference to the controller database connection pool
     * @param secDb Reference to the security database accessor
     * @throws AccessDeniedException Thrown if the specified access context is not authorized
     * @throws DatabaseException if a database error occurs
     */
    public static <T extends ControllerDatabase> void set(
        AccessContext accCtx,
        SecurityLevel newLevel,
        @Nullable T ctrlDb,
        @Nullable DbAccessor<T> secDb
    )
        throws AccessDeniedException, DatabaseException
    {
        accCtx.getEffectivePrivs().requirePrivileges(Privilege.PRIV_SYS_ALL);

        if (ctrlDb != null && secDb != null)
        {
            secDb.setSecurityLevel(ctrlDb, newLevel);
        }

        GLOBAL_SEC_LEVEL_REF.set(newLevel);
    }

    /**
     * Sets the configured security level from the database.
     *
     * Runs upon initial startup and during reconfiguration
     *
     * @param secLvlValue
     *
     * @throws DatabaseException if a database error occurs
     */
    static void setLoadedSecLevel(String secLvlValue)
    {
        if (secLvlValue != null)
        {
            if (NO_SECURITY.name().equals(secLvlValue))
            {
                GLOBAL_SEC_LEVEL_REF.set(NO_SECURITY);
            }
            else
            if (RBAC.name().equals(secLvlValue))
            {
                GLOBAL_SEC_LEVEL_REF.set(RBAC);
            }
            else
            if (MAC.name().equals(secLvlValue))
            {
                GLOBAL_SEC_LEVEL_REF.set(MAC);
            }
            else
            {
                // TODO: A warning should be logged when an unknown value is encountered
                System.err.println(String.format("Unknown security level '%s' set.", secLvlValue));
            }
        }
    }
}
