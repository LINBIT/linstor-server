package com.linbit.linstor.security;

import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.dbdrivers.derby.DerbyConstants;
import com.linbit.linstor.logging.ErrorReporter;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.event.Level;

/**
 * Identity authentication
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class Authentication
{
    public static final String HASH_ALGORITHM = "SHA-512";
    public static final int HASH_SIZE = 64;
    public static final int SALT_SIZE = 16;

    private static final AtomicBoolean GLOBAL_AUTH_REQUIRED =
        new AtomicBoolean(true);

    private MessageDigest hashAlgo;

    private ControllerDatabase ctrlDb;
    private DbAccessor dbDriver;
    private ErrorReporter errorLog;

    public Authentication(
        AccessContext accCtx,
        ControllerDatabase ctrlDbRef,
        DbAccessor dbDriverRef,
        ErrorReporter errorLogRef
    )
        throws AccessDeniedException, NoSuchAlgorithmException
    {
        ErrorCheck.ctorNotNull(Authentication.class, AccessContext.class, accCtx);
        ErrorCheck.ctorNotNull(Authentication.class, ControllerDatabase.class, ctrlDbRef);
        ErrorCheck.ctorNotNull(Authentication.class, DbAccessor.class, dbDriverRef);

        accCtx.getEffectivePrivs().requirePrivileges(Privilege.PRIV_SYS_ALL);
        hashAlgo = MessageDigest.getInstance(HASH_ALGORITHM);

        ctrlDb = ctrlDbRef;
        dbDriver = dbDriverRef;
        errorLog = errorLogRef;
    }

    public static boolean isRequired()
    {
        return GLOBAL_AUTH_REQUIRED.get();
    }

    public static void setRequired(
        AccessContext accCtx,
        boolean newPolicy,
        ControllerDatabase ctrlDb,
        DbAccessor secDb
    )
        throws AccessDeniedException, SQLException
    {
        accCtx.getEffectivePrivs().requirePrivileges(Privilege.PRIV_SYS_ALL);

        // Always commit if the change is non-persistent / temporary
        boolean committed = ctrlDb == null || secDb == null;

        if (ctrlDb != null && secDb != null)
        {
            Connection dbConn = null;
            try
            {
                dbConn = ctrlDb.getConnection();
                secDb.setAuthRequired(dbConn, newPolicy);
                committed = true;
            }
            finally
            {
                ctrlDb.returnConnection(dbConn);
            }
        }
        
        if (committed)
        {
            GLOBAL_AUTH_REQUIRED.set(newPolicy);
        }
    }

    public AccessContext signIn(IdentityName idName, byte[] password)
        throws SignInException, InvalidNameException
    {
        Connection dbConn = null;
        AccessContext signInCtx = null;
        try
        {
            dbConn = ctrlDb.getConnection();
            if (dbConn == null)
            {
                throw new SQLException(
                    "The controller database connection pool failed to provide a database connection"
                );
            }
            // Query the identity entry
            ResultSet signInEntry = dbDriver.getSignInEntry(dbConn, idName);

            // Position cursor on the first row
            if (signInEntry.next())
            {
                final String storedIdStr = signInEntry.getString(DerbyConstants.IDENTITY_NAME);
                final String storedDfltRoleStr = signInEntry.getString(DerbyConstants.ROLE_NAME);
                final String storedDfltTypeStr = signInEntry.getString(DerbyConstants.DOMAIN_NAME);
                final Long storedDfltRolePrivs = signInEntry.getLong(DerbyConstants.ROLE_PRIVILEGES);

                byte[] storedSalt = signInEntry.getBytes(DerbyConstants.PASS_SALT);
                byte[] storedHash = signInEntry.getBytes(DerbyConstants.PASS_HASH);

                if (storedIdStr == null)
                {
                    String reportId = errorLog.reportError(
                        Level.ERROR,
                        new SignInException(
                            "Sign-in failed: Database error: The identity field of the requested " +
                            "database record contains a NULL value",
                            // Description
                            "Sign-in failed due to a database error",
                            // Cause
                            "The database record for the identity is invalid",
                            // Correction
                            "This error may require operator or developer intervention.\n" +
                            "Check the database table layout and constraints.",
                            // Error details
                            "The identity field of the database record contains a NULL value.\n" +
                            "This error indicates a severe problem with the database, as it should " +
                            "normally be prevented by database constraints."
                        )
                    );
                    dbFailed(reportId);
                }

                IdentityName storedIdName = new IdentityName(storedIdStr);

                if (!idName.equals(storedIdName))
                {
                    String reportId = errorLog.reportError(
                        Level.ERROR,
                        new SignInException(
                            String.format(
                                "Sign-in failed: Database error: Identity '%s' in the database record " +
                                "does not match requested identity '%s'",
                                storedIdName.getName(), idName.getName()
                            ),
                            // Description
                            "Sign-in failed due to a database error",
                            // Cause
                            "The database record for the identity is invalid",
                            // Correction
                            "This error may require operator or developer intervention.\n" +
                            "Check the database table layout and constraints.\n" +
                            "Check for known bugs in this version of " + LinStor.PROGRAM,
                            // Error details
                            "The database query yielded a record for another identity than the one " +
                            "for which a record was requested.\n" +
                            "This error indicates a severe problem with the database or this version " +
                            "of " + LinStor.PROGRAM
                        )
                    );
                    dbFailed(reportId);
                }

                if (Authentication.passwordMatches(hashAlgo, password, storedSalt, storedHash))
                {
                    Identity signInIdentity = Identity.get(storedIdName);
                    if (signInIdentity == null)
                    {
                        String reportId = errorLog.reportError(
                            Level.ERROR,
                            new SignInException(
                                String.format(
                                    "Sign-in failed: Unable to find identity '%s' although it is " +
                                    "referenced in the security database",
                                    storedIdName.value
                                ),
                                // Description
                                "Sign-in failed due to a data consistency error",
                                // Cause
                                "The list of identities loaded into the program does not match the " +
                                "list of identities in the database",
                                // Correction
                                "Reload the program's in-memory data from the database or " +
                                "restart the program.",
                                // Error details
                                String.format(
                                    "A record for the identity '%s' was found in the database, but the " +
                                    "same identity is not present in the program's in-memory data.",
                                    storedIdName.value
                                )
                            )
                        );
                        dbFailed(reportId);
                    }

                    // Default to no privileges
                    long signInPrivMask = 0;
                    // Default to the PUBLIC role if no default role is listed
                    Role signInRole = Role.PUBLIC_ROLE;
                    // Default to the PUBLIC type if no default type is available
                    // (because no default role is listed)
                    SecurityType signInType = SecurityType.PUBLIC_TYPE;
                    if (storedDfltRoleStr != null)
                    {
                        RoleName storedDfltRoleName = new RoleName(storedDfltRoleStr);
                        signInRole = Role.get(storedDfltRoleName);
                        if (signInRole == null)
                        {
                            String reportId = errorLog.reportError(
                                Level.ERROR,
                                new SignInException(
                                    String.format(
                                        "Sign-in failed: Unable to find role '%s' although it is " +
                                        "referenced in the security database",
                                        storedDfltRoleName.value
                                    ),
                                    // Description
                                    "Sign-in failed due to a data consistency error",
                                    // Cause
                                    "The list of roles loaded into the program does not match the " +
                                    "list of roles in the database",
                                    // Correction
                                    "Reload the program's in-memory data from the database or " +
                                    "restart the program.",
                                    // Error details
                                    String.format(
                                        "A record for the role '%s' was found in the database, but the " +
                                        "same role is not present in the program's in-memory data.",
                                        storedDfltRoleName.value
                                    )
                                )
                            );
                            dbFailed(reportId);
                        }
                        // If a default role is listed, then the default domain field
                        // must contain a value
                        if (storedDfltTypeStr == null)
                        {
                            String reportId = errorLog.reportError(
                                Level.ERROR,
                                new SignInException(
                                    String.format(
                                        "Sign-in failed: Database error: Security domain field for " +
                                        "default role '%s' contains a NULL value",
                                        storedDfltRoleName.value
                                    ),
                                    "Sign-in failed due to a database error",
                                    // Cause
                                    "The database record for the default role is invalid",
                                    // Correction
                                    "This error may require operator or developer intervention.\n" +
                                    "Check the database table layout and constraints.",
                                    // Error details
                                    String.format(
                                        "The security domain field of the database record for the role '%s' " +
                                        "contains a NULL value.\n" +
                                        "This error indicates a severe problem with the database, as it should " +
                                        "normally be prevented by database constraints.",
                                        storedDfltRoleName.value
                                    )
                                )
                            );
                            dbFailed(reportId);
                        }
                        SecTypeName storedDfltTypeName = new SecTypeName(storedDfltTypeStr);
                        signInType = SecurityType.get(storedDfltTypeName);
                        if (signInType == null)
                        {
                            String reportId = errorLog.reportError(
                                Level.ERROR,
                                new SignInException(
                                    String.format(
                                        "Sign-in failed: Unable to find security domain '%s' " +
                                        "although it is referenced in the security database",
                                        storedDfltTypeName.value
                                    ),
                                    // Description
                                    "Sign-in failed due to a data consistency error",
                                    // Cause
                                    "The list of security domain/types loaded into the program does not match the " +
                                    "list of security domains/types in the database",
                                    // Correction
                                    "Reload the program's in-memory data from the database or " +
                                    "restart the program.",
                                    // Error details
                                    String.format(
                                        "A record for the security domain '%s' was found in the database, but the " +
                                        "same security domain is not present in the program's in-memory data.",
                                        storedDfltTypeName.value
                                    )
                                )
                            );
                            dbFailed(reportId);
                        }
                        // If a default role is listed, then the privilege field must
                        // contain a value
                        if (storedDfltRolePrivs == null)
                        {
                            String reportId = errorLog.reportError(
                                Level.ERROR,
                                new SignInException(
                                    String.format(
                                        "Sign-in failed: Database error: Privileges field for " +
                                        "default role '%s' is NULL",
                                        storedDfltRoleName.value
                                    ),
                                    "Sign-in failed due to a database error",
                                    // Cause
                                    "The database record for the default role is invalid",
                                    // Correction
                                    "This error may require operator or developer intervention.\n" +
                                    "Check the database table layout and constraints.",
                                    // Error details
                                    String.format(
                                        "The privileges field of the database record for the role '%s' " +
                                        "contains a NULL value.\n" +
                                        "This error indicates a severe problem with the database, as it should " +
                                        "normally be prevented by database constraints.",
                                        storedDfltRoleName.value
                                    )
                                )
                            );
                            dbFailed(reportId);
                        }
                        signInPrivMask = storedDfltRolePrivs;
                        // Create the requested AccessContext object
                    }

                    // Create the AccessContext instance
                    signInCtx = new AccessContext(
                        signInIdentity,
                        signInRole,
                        signInType,
                        new PrivilegeSet(signInPrivMask)
                    );
                }
                else
                {
                    // The password does not match the one that is stored in the database
                    // Abort with an InvalidCredentialsException
                    signInFailed();
                }
            }
            else
            {
                signInFailed();
            }
        }
        catch (SQLException sqlExc)
        {
            String reportId = errorLog.reportError(
                Level.ERROR,
                new SignInException(
                    "Sign-in failed: Database error: The SQL query for the security database record failed",
                    // Description
                    "Sign-in failed due to a database error",
                    // Cause
                    "The database query for the security database record failed",
                    null,
                    // No error details
                    null,
                    sqlExc
                )
            );
            dbFailed(reportId);
        }
        finally
        {
            ctrlDb.returnConnection(dbConn);
        }
        return signInCtx;
    }

    /**
     * Checks whether the hash created by salting and hashing the password
     * matches the specified salted hash
     *
     * {@code password}, {@code storedSalt}, {@code storedHash} may be null.
     *
     * Upon exit from this method, the contents of any supplied {@code password},
     * {@code storedSalt} and {@code storedHash} arrays will be cleared
     * (all bytes in the array will be set to zero).
     *
     * @param dgstAlgo The digest algorithm used for hashing
     * @param password The plaintext password to check
     * @param storedSalt The salt used for hashing
     * @param storedHash The stored hash to compare the password with
     * @return True if the password matches (is correct), false otherwise
     */
    static boolean passwordMatches(
        MessageDigest dgstAlgo,
        byte[] password,
        byte[] storedSalt,
        byte[] storedHash
    )
    {
        boolean matchFlag = false;

        if (password != null && storedSalt != null && storedHash != null)
        {
            byte[] enteredPasswordHash = null;
            // Hash the password that was supplied for the signin
            synchronized (dgstAlgo)
            {
                dgstAlgo.update(storedSalt);
                enteredPasswordHash = dgstAlgo.digest(password);
            }

            if (enteredPasswordHash != null)
            {
                if (enteredPasswordHash.length == storedHash.length)
                {
                    int idx = 0;
                    while (idx < storedHash.length)
                    {
                        if (enteredPasswordHash[idx] != storedHash[idx])
                        {
                            break;
                        }
                        ++idx;
                    }
                    if (idx == storedHash.length)
                    {
                        matchFlag = true;
                    }
                }
            }
            clearDataFields(enteredPasswordHash);
        }
        clearDataFields(password, storedSalt, storedHash);

        return matchFlag;
    }

    /**
     * Clears the specified byte arrays by setting all elements to zero.
     *
     * Any element of {@code dataFieldList} may be a null reference.
     * The {@code dataFieldList} argument itself may NOT be a null reference.
     * @param dataFieldList The list of byte arrays to clear
     */
    static void clearDataFields(byte[]... dataFieldList)
    {
        for (byte[] dataField : dataFieldList)
        {
            if (dataField != null)
            {
                Arrays.fill(dataField, (byte) 0);
            }
        }
    }

    private static void dbFailed(String reportId)
        throws SignInException
    {
        String corrText = null;
        String detailsText = null;
        if (reportId != null)
        {
            corrText = "If the problem persists, review the error report on the server for details about\n" +
                       "the cause of the error.";
            detailsText = "The error report was filed under report ID " + reportId;
        }
        throw new SignInException(
            "Sign-in failed due to a database error",
            // Description
            "Sign-in failed",
            // Cause
            "The system encountered a database error while attempting to perform the sign-in",
            // Correction
            corrText,
            // No error details
            detailsText
        );
    }

    private static void signInFailed()
        throws InvalidCredentialsException
    {
        throw new InvalidCredentialsException(
            "Sign-in failed: Invalid signin credentials",
            // Description
            "Sign-in failed",
            // Cause
            "The credentials for the sign-in are not valid",
            // Correction
            "The name of a valid identity and a matching password must be provided " +
            "to sign in to the system",
            // No error details
            null
        );
    }

    static void load(ControllerDatabase ctrlDb, DbAccessor secDb)
        throws SQLException
    {
        Connection dbConn = null;
        GLOBAL_AUTH_REQUIRED.set(true);
        try
        {
            dbConn = ctrlDb.getConnection();
            if (dbConn == null)
            {
                throw new SQLException(
                    "The controller database connection pool failed to provide a database connection"
                );
            }

            ResultSet rslt = secDb.loadAuthRequired(dbConn);
            if (rslt.next())
            {
                String authPlcKey = rslt.getString(1);
                String authPlcVal = rslt.getString(2);

                if (!authPlcKey.equals(SecurityDbConsts.KEY_AUTH_REQ))
                {
                    throw new ImplementationError(
                        "Security level database query returned incorrect key '" + authPlcKey + "'\n" +
                        "instead of expected key '" + SecurityDbConsts.KEY_AUTH_REQ + "'",
                        null
                    );
                }

                if (Boolean.toString(false).equalsIgnoreCase(authPlcVal))
                {
                    GLOBAL_AUTH_REQUIRED.set(false);
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
