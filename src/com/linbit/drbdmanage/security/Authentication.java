package com.linbit.drbdmanage.security;

import com.linbit.ErrorCheck;
import com.linbit.InvalidNameException;
import com.linbit.drbdmanage.ControllerDatabase;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

/**
 * Identity authentication
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
class Authentication
{
    public final static String HASH_ALGORITHM = "SHA-512";
    public final static int HASH_SIZE = 64;
    public final static int SALT_SIZE = 16;

    private MessageDigest hashAlgo;

    private ControllerDatabase ctrlDb;
    private DbAccessor dbDriver;

    Authentication(AccessContext accCtx, ControllerDatabase ctrlDbRef, DbAccessor dbDriverRef)
        throws AccessDeniedException, NoSuchAlgorithmException
    {
        ErrorCheck.ctorNotNull(Authentication.class, AccessContext.class, accCtx);
        ErrorCheck.ctorNotNull(Authentication.class, ControllerDatabase.class, ctrlDbRef);
        ErrorCheck.ctorNotNull(Authentication.class, DbAccessor.class, dbDriverRef);

        accCtx.getEffectivePrivs().requirePrivileges(Privilege.PRIV_SYS_ALL);
        hashAlgo = MessageDigest.getInstance(HASH_ALGORITHM);

        ctrlDb = ctrlDbRef;
        dbDriver = dbDriverRef;
    }

    public AccessContext signIn(IdentityName idName, byte[] password)
        throws SignInException, InvalidNameException
    {
        Connection dbConn = null;
        if (dbConn == null)
        {
            throw new SignInException(
                new SQLException(
                    "The controller database connection pool failed to provide a database connection"
                )
            );
        }
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
                String storedIdStr = signInEntry.getString(SecurityDbFields.IDENTITY_NAME);
                String storedDfltRoleStr = signInEntry.getString(SecurityDbFields.ROLE_NAME);
                String storedDfltTypeStr = signInEntry.getString(SecurityDbFields.TYPE_NAME);
                Long storedDfltRolePrivs = signInEntry.getLong(SecurityDbFields.ROLE_PRIVILEGES);

                byte[] storedSalt = signInEntry.getBytes(SecurityDbFields.PASS_SALT);
                byte[] storedHash = signInEntry.getBytes(SecurityDbFields.PASS_HASH);

                if (storedIdStr == null)
                {
                    throw new SignInException(
                        "Sign in failed: Database error: The identity field of the requested " +
                        "database record contains a NULL value"
                    );
                }

                IdentityName storedIdName = new IdentityName(storedIdStr);

                if (!idName.equals(storedIdName))
                {
                    throw new SignInException(
                        String.format(
                            "Sign in failed: Database error: Identity '%s' in the database record " +
                            "does not match requested identity '%s'",
                            storedIdName.getName(), idName.getName()
                        )
                    );
                }

                byte[] enteredPasswordHash = null;
                // Hash the password that was supplied for the signin
                if (storedSalt != null && storedHash != null)
                {
                    synchronized (hashAlgo)
                    {
                        hashAlgo.update(storedSalt);
                        enteredPasswordHash = hashAlgo.digest(password);
                    }
                    Arrays.fill(password, (byte) 0);
                }
                else
                {
                    // No password is set on the identity entry, cannot sign in
                    Arrays.fill(password, (byte) 0);
                    signInFailed();
                }

                boolean hashesMatch = false;
                if (enteredPasswordHash != null && storedHash != null)
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
                            hashesMatch = true;
                        }
                    }
                }

                if (hashesMatch)
                {
                    Identity signInIdentity = Identity.get(storedIdName);
                    if (signInIdentity == null)
                    {
                        throw new SignInException(
                            String.format(
                                "Sign in failed: Unable to find identity '%s' although it is " +
                                "referenced in the security database",
                                storedIdName.value
                            )
                        );
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
                            throw new SignInException(
                                String.format(
                                    "Sign in failed: Unable to find role '%s' although it is " +
                                    "referenced in the security database",
                                    storedDfltRoleName.value
                                )
                            );
                        }
                        // If a default role is listed, then the default domain field
                        // must contain a value
                        if (storedDfltTypeStr == null)
                        {
                            throw new SignInException(
                                String.format(
                                    "Sign in failed: Database error: Security domain field for " +
                                    "default role '%s' contains a NULL value",
                                    storedDfltRoleName.value
                                )
                            );
                        }
                        SecTypeName storedDfltTypeName = new SecTypeName(storedDfltTypeStr);
                        signInType = SecurityType.get(storedDfltTypeName);
                        if (signInType == null)
                        {
                            throw new SignInException(
                                String.format(
                                    "Sign in failed: Unable to find security domain '%s' " +
                                    "although it is referenced in the security database",
                                    storedDfltTypeName.value
                                )
                            );
                        }
                        // If a default role is listed, then the privilege field must
                        // contain a value
                        if (storedDfltRolePrivs == null)
                        {
                            throw new SignInException(
                                String.format(
                                    "Sign in failed: Database error: Privileges field for " +
                                    "default role '%s' is NULL",
                                    storedDfltRoleName.value
                                )
                            );
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
            }
            else
            {
                signInFailed();
            }
        }
        catch (SQLException sqlExc)
        {
            throw new SignInException(
                "Signin failed: Database error: The SQL query for the security database record failed",
                sqlExc
            );
        }
        finally
        {
            ctrlDb.returnConnection(dbConn);
        }
        return signInCtx;
    }

    private static void signInFailed()
        throws InvalidCredentialsException
    {
        throw new InvalidCredentialsException(
            "SignIn failed: Invalid signin credentials"
        );
    }
}
