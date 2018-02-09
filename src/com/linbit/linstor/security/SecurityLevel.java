package com.linbit.linstor.security;

import com.linbit.ImplementationError;
import com.linbit.linstor.ControllerDatabase;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
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
     * @throws SQLException if a database error occurs
     */
    public static void set(
        AccessContext accCtx,
        SecurityLevel newLevel,
        ControllerDatabase ctrlDb,
        DbAccessor secDb
    )
        throws AccessDeniedException, SQLException
    {
        accCtx.getEffectivePrivs().requirePrivileges(Privilege.PRIV_SYS_ALL);
        if (ctrlDb != null && secDb != null)
        {
            Connection dbConn = null;
            try
            {
                dbConn = ctrlDb.getConnection();
                secDb.setSecurityLevel(dbConn, newLevel);
                GLOBAL_SEC_LEVEL_REF.set(newLevel);
            }
            finally
            {
                ctrlDb.returnConnection(dbConn);
            }
        }
    }

    /**
     * Loads the configured security level from the database.
     *
     * Runs upon initial startup and during reconfiguration
     *
     * @param ctrlDb Reference to the controller database connection pool
     * @param secDb Reference to the security database accessor
     * @throws SQLException if a database error occurs
     */
    static void load(ControllerDatabase ctrlDb, DbAccessor secDb)
        throws SQLException
    {
        Connection dbConn = null;
        try
        {
            dbConn = ctrlDb.getConnection();
            if (dbConn == null)
            {
                throw new SQLException(
                    "The controller database connection pool failed to provide a database connection"
                );
            }

            ResultSet rslt = secDb.loadSecurityLevel(dbConn);
            if (rslt.next())
            {
                String secLvlKey = rslt.getString(1);
                String secLvlValue = rslt.getString(2);

                if (!secLvlKey.equals(DbDerbyPersistence.KEY_SEC_LEVEL))
                {
                    throw new ImplementationError(
                        "Security level database query returned incorrect key '" + secLvlKey + "'\n" +
                        "instead of expected key '" + DbDerbyPersistence.KEY_SEC_LEVEL + "'",
                        null
                    );
                }

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
                }
            }
        }
        finally
        {
            ctrlDb.returnConnection(dbConn);
        }
    }
}
