package com.linbit.linstor.security;

import com.linbit.ErrorCheck;
import com.linbit.InvalidNameException;
import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.LinstorConfigToml;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.data.SignInEntry;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Hashtable;

import org.slf4j.event.Level;

public class CtrlAuthentication
{
    private MessageDigest hashAlgo;

    private final ControllerDatabase ctrlDb;
    private final DbAccessor dbDriver;
    private final ErrorReporter errorLog;
    private final AccessContext publicCtx;

    private LinstorConfigToml.LDAP ldapConfig;

    public CtrlAuthentication(
        AccessContext initCtx,
        AccessContext publicCtxRef,
        ControllerDatabase ctrlDbRef,
        DbAccessor dbDriverRef,
        ErrorReporter errorLogRef,
        LinstorConfigToml linstorConfigTomlRef
    )
        throws AccessDeniedException, NoSuchAlgorithmException
    {
        ErrorCheck.ctorNotNull(CtrlAuthentication.class, AccessContext.class, initCtx);
        ErrorCheck.ctorNotNull(CtrlAuthentication.class, ControllerDatabase.class, ctrlDbRef);
        ErrorCheck.ctorNotNull(CtrlAuthentication.class, DbAccessor.class, dbDriverRef);

        initCtx.getEffectivePrivs().requirePrivileges(Privilege.PRIV_SYS_ALL);
        hashAlgo = MessageDigest.getInstance(Authentication.HASH_ALGORITHM);

        ctrlDb = ctrlDbRef;
        dbDriver = dbDriverRef;
        errorLog = errorLogRef;
        publicCtx = publicCtxRef;

        ldapConfig = linstorConfigTomlRef.getLDAP();
    }

    private AccessContext signInLinstor(IdentityName idName, byte[] password)
        throws SignInException, InvalidNameException
    {
        AccessContext signInCtx = null;
        try
        {
            // Query the identity entry
            SignInEntry signInEntry = dbDriver.getSignInEntry(ctrlDb, idName);

            // Position cursor on the first row
            if (signInEntry != null)
            {
                final String storedIdStr = signInEntry.getIdentityName();
                final String storedDfltRoleStr = signInEntry.getRoleName();
                final String storedDfltTypeStr = signInEntry.getDomainName();
                final Long storedDfltRolePrivs = signInEntry.getRolePrivileges();

                byte[] storedSalt;
                byte[] storedHash;
                try
                {
                    storedSalt = signInEntry.getSalt();
                    storedHash = signInEntry.getHash();
                }
                catch (IllegalArgumentException exc)
                {
                    throw new SignInException("Invalid password salt or hash value in database", exc);
                }
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
        catch (DatabaseException dbExc)
        {
            String reportId = errorLog.reportError(
                Level.ERROR,
                new SignInException(
                    "Sign-in failed: Database error: The database query for the security database record failed",
                    // Description
                    "Sign-in failed due to a database error",
                    // Cause
                    "The database query for the security database record failed",
                    null,
                    // No error details
                    null,
                    dbExc
                )
            );
            dbFailed(reportId);
        }
        return signInCtx;
    }

    private AccessContext signInLDAP(IdentityName idName, byte[] password)
        throws SignInException
    {
        AccessContext signInContext = null;

        Hashtable<String, String> ldapEnv = new Hashtable<>();
        ldapEnv.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        ldapEnv.put(Context.PROVIDER_URL, ldapConfig.getUri());
        ldapEnv.put(Context.SECURITY_AUTHENTICATION, "simple");
        String ldapDN = ldapConfig.getDN().replaceAll("\\{user}", idName.displayValue);
        ldapEnv.put(Context.SECURITY_PRINCIPAL, ldapDN);
        ldapEnv.put(Context.SECURITY_CREDENTIALS, new String(password, StandardCharsets.UTF_8));

        try
        {
            DirContext ctx = new InitialDirContext(ldapEnv);

            if (!ldapConfig.getSearchFilter().isEmpty())
            {
                SearchControls searchControls = new SearchControls();
                searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);

                final String searchFilter = ldapConfig.getSearchFilter().replaceAll("\\{user}", idName.displayValue);

                NamingEnumeration result = ctx.search(ldapConfig.getSearchBase(), searchFilter, searchControls);

                if (!result.hasMore())
                {
                    result.close();
                    throw new InvalidCredentialsException(
                        "Sign-in failed: LDAP search filter didn't find a match.",
                        // Description
                        "Sign-in failed",
                        // Cause
                        "Search filter expression didn't match any item.",
                        // Correction
                        "Adapt LDAP search_base,search_filter or add user to searched group.",
                        // No error details
                        null
                    );
                }

                result.close();
            }

            signInContext = publicCtx;
            errorLog.logInfo("LDAP User %s successfully authenticated.", idName.displayValue);
        }
        catch (NamingException nExc)
        {
            throw new InvalidCredentialsException(
                "Sign-in failed: Invalid sign in credentials",
                // Description
                "Sign-in failed",
                // Cause
                "The credentials for the sign-in are not valid or LDAP access not correctly configured.",
                // Correction
                "The name of a valid identity and a matching password must be provided " +
                    "to sign in to the system or LDAP access correctly configured.",
                nExc.getMessage(),
                nExc
            );
        }
        return signInContext;
    }

    public AccessContext signIn(IdentityName idName, byte[] password)
        throws SignInException, InvalidNameException
    {
        AccessContext accCtx;

        if (ldapConfig.isEnabled())
        {
            accCtx = signInLDAP(idName, password);
        }
        else
        {
            accCtx = signInLinstor(idName, password);
        }

        return accCtx;
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
}
