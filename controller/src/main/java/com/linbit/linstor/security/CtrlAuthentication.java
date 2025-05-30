package com.linbit.linstor.security;

import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.crypto.KeyDerivation;
import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.modularcrypto.ModularCryptoProvider;
import com.linbit.linstor.security.pojo.SignInEntryPojo;

import javax.management.relation.Role;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;

import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.security.Identity;
import java.util.Hashtable;

import org.slf4j.event.Level;

public class CtrlAuthentication<CTRL_DB_TYPE extends ControllerDatabase>
{
    private final CTRL_DB_TYPE ctrlDb;
    private final DbAccessor<CTRL_DB_TYPE> dbDriver;
    private final ErrorReporter errorLog;
    private final AccessContext publicCtx;
    private final AccessContext sysCtx;

    private CtrlConfig ctrlCfg;

    private KeyDerivation keyDrv;

    CtrlAuthentication(
        AccessContext initCtx,
        AccessContext sysCtxRef,
        AccessContext publicCtxRef,
        CTRL_DB_TYPE ctrlDbRef,
        DbAccessor<CTRL_DB_TYPE> dbDriverRef,
        ModularCryptoProvider cryptoProvider,
        ErrorReporter errorLogRef,
        CtrlConfig ctrlCfgRef
    )
        throws LinStorException
    {
        dbDriver = dbDriverRef;
        ErrorCheck.ctorNotNull(CtrlAuthentication.class, AccessContext.class, initCtx);
        ErrorCheck.ctorNotNull(CtrlAuthentication.class, ControllerDatabase.class, ctrlDbRef);

        initCtx.getEffectivePrivs().requirePrivileges(Privilege.PRIV_SYS_ALL);

        ctrlDb = ctrlDbRef;
        errorLog = errorLogRef;
        sysCtx = sysCtxRef;
        publicCtx = publicCtxRef;

        ctrlCfg = ctrlCfgRef;

        keyDrv = cryptoProvider.createKeyDerivation();
    }

    private AccessContext signInLinstor(IdentityName idName, byte[] password)
        throws SignInException, InvalidNameException
    {
        AccessContext signInCtx = null;
        try
        {
            // Query the identity entry
            final SignInEntryPojo signInEntry = dbDriver.getSignInEntry(ctrlDb, idName);
            if (signInEntry != null)
            {
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

                if (Authentication.passwordMatches(keyDrv, password, storedSalt, storedHash))
                {
                    final IdentityName storedIdName = idIntegrityCheck(signInEntry, idName);
                    final Identity signInId = getIdentity(storedIdName);

                    // Default to the PUBLIC role if no default role is listed
                    Role signInRole = Role.PUBLIC_ROLE;

                    // Default to the PUBLIC type if no default type is available
                    // (because no default role is listed)
                    SecurityType signInType = SecurityType.PUBLIC_TYPE;

                    // Default to no privileges
                    long signInPrivs = 0L;

                    final String storedDfltRoleStr = signInEntry.getRoleName();
                    if (storedDfltRoleStr != null)
                    {
                        RoleName storedDfltRoleName = new RoleName(storedDfltRoleStr);
                        signInRole = getRoleByName(storedDfltRoleName);
                        signInType = getSecTypeFromEntry(signInEntry, storedDfltRoleName);
                        signInPrivs = signInEntry.getRolePrivileges();
                    }

                    // Create the AccessContext instance
                    signInCtx = new AccessContext(
                        signInId,
                        signInRole,
                        signInType,
                        new PrivilegeSet(signInPrivs)
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
                // There is no entry for this identity in the database
                // Abort with an InvalidCredentialsException
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
        throws SignInException, InvalidNameException
    {
        AccessContext signInContext = null;

        Hashtable<String, String> ldapEnv = new Hashtable<>();
        ldapEnv.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        ldapEnv.put(Context.PROVIDER_URL, ctrlCfg.getLdapUri());
        ldapEnv.put(Context.SECURITY_AUTHENTICATION, "simple");
        String escapedUserName = escapeLdapName(idName.displayValue);
        String ldapDN = ctrlCfg.getLdapDn().replaceAll("\\{user}", idName.displayValue);
        ldapEnv.put(Context.SECURITY_PRINCIPAL, ldapDN);
        ldapEnv.put(Context.SECURITY_CREDENTIALS, new String(password, StandardCharsets.UTF_8));

        try
        {
            DirContext ctx = new InitialDirContext(ldapEnv);

            if (!ctrlCfg.getLdapSearchFilter().isEmpty())
            {
                SearchControls searchControls = new SearchControls();
                searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);

                final String searchFilter = ctrlCfg.getLdapSearchFilter().replaceAll("\\{user}", escapeLdapSearchFilter(idName.displayValue));

                NamingEnumeration result = ctx.search(ctrlCfg.getLdapSearchFilter(), searchFilter, searchControls);

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

                return signInContext;
            }

            // Helper method to escape LDAP DN values
            private String escapeLdapName(String name) {
                return name.replaceAll("([,+\"\\\\<>;=\\x00-\\x1f\\x7f-\\x9f])", "\\\\$1");
            }
            
            // Helper method to escape LDAP search filter values  
            private String escapeLdapSearchFilter(String filter) {
                return filter.replaceAll("([\\\\*\\(\\)\\x00])", "\\\\$1");
            }

            final SignInEntryPojo signInEntry = dbDriver.getSignInEntry(ctrlDb, idName);

            // Default to the PUBLIC role if no default role is listed
            Role signInRole = Role.PUBLIC_ROLE;

            // Default to the PUBLIC type if no default type is available
            // (because no default role is listed)
            SecurityType signInDomain = SecurityType.PUBLIC_TYPE;

            // Default to no privileges
            long signInPrivs = 0L;

            Identity signInId;
            if (signInEntry != null)
            {
                // Load default role, security domain and privileges from the Identity entry in the database
                final IdentityName storedIdName = idIntegrityCheck(signInEntry, idName);
                signInId = getIdentity(storedIdName);

                final String storedDfltRoleStr = signInEntry.getRoleName();
                if (storedDfltRoleStr != null)
                {
                    final RoleName storedDfltRoleName = new RoleName(storedDfltRoleStr);
                    signInRole = getRoleByName(storedDfltRoleName);
                    signInDomain = getSecTypeFromEntry(signInEntry, signInRole.name);
                    signInPrivs = signInEntry.getRolePrivileges();
                }
            }
            else
            {
                // Create an Identity entry in the database, use the PUBLIC role and the PUBLIC security domain
                dbDriver.createSignInEntry(
                    Identity.get(idName),
                    Role.PUBLIC_ROLE,
                    new byte[0],
                    new byte[0]
                );

                AccessContext privCtx = sysCtx.clone();
                PrivilegeSet effPrivSet = privCtx.getEffectivePrivs();
                effPrivSet.enablePrivileges(Privilege.PRIV_SYS_ALL);
                signInId = Identity.create(privCtx, idName);
            }

            signInContext = new AccessContext(
                signInId,
                signInRole,
                signInDomain,
                new PrivilegeSet(signInPrivs)
            );
            errorLog.logInfo("LDAP User %s successfully authenticated.", idName.displayValue);
        }
        catch (AccessDeniedException accExc)
        {
            throw new ImplementationError(
                "Sign-in failed: The system context lacks the required privileges to process the sign-in request. " +
                "This is an implementation error.",
                accExc
            );
        }
        catch (DatabaseException dbExc)
        {
            throw new SignInException(
                "The sign-in request failed due to a database error",
                "Sign-in failed",
                "A database error occured while processing the sign-in request",
                "If an external database is used, make sure that the connection to the database server is available",
                dbExc.getMessage(),
                dbExc
            );
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

        if (ctrlCfg.isLdapEnabled())
        {
            accCtx = signInLDAP(idName, password);
        }
        else
        {
            accCtx = signInLinstor(idName, password);
        }

        return accCtx;
    }

    private IdentityName idIntegrityCheck(SignInEntryPojo signInEntry, IdentityName idName)
        throws SignInException, DatabaseException, InvalidNameException
    {
        final String storedIdStr = signInEntry.getIdentityName();
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
        return storedIdName;
    }

    private Identity getIdentity(IdentityName idName)
        throws SignInException
    {
        Identity signInIdentity = Identity.get(idName);
        if (signInIdentity == null)
        {
            String reportId = errorLog.reportError(
                Level.ERROR,
                new SignInException(
                    String.format(
                        "Sign-in failed: Unable to find identity '%s' although it is " +
                            "referenced in the security database",
                        idName.value
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
                        idName.value
                    )
                )
            );
            dbFailed(reportId);
        }
        return signInIdentity;
    }

    private Role getRoleByName(RoleName rlName)
        throws SignInException, InvalidNameException
    {
        Role signInRole = Role.get(rlName);
        if (signInRole == null)
        {
            String reportId = errorLog.reportError(
                Level.ERROR,
                new SignInException(
                    String.format(
                        "Sign-in failed: Unable to find role '%s' although it is " +
                            "referenced in the security database",
                        rlName.value
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
                        rlName.value
                    )
                )
            );
            dbFailed(reportId);
        }
        return signInRole;
    }

    /**
     * Returns the security domain for the default role.
     *
     * This method should only be called if the default role is set
     */
    private SecurityType getSecTypeFromEntry(SignInEntryPojo signInEntry, RoleName rlName)
        throws SignInException, InvalidNameException
    {
        final String domainStr = signInEntry.getDomainName();
        if (domainStr == null)
        {
            String reportId = errorLog.reportError(
                Level.ERROR,
                new SignInException(
                    String.format(
                        "Sign-in failed: Database error: Security domain field for " +
                            "default role '%s' contains a NULL value",
                        rlName.value
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
                        rlName.value
                    )
                )
            );
            dbFailed(reportId);
        }
        SecTypeName storedDfltTypeName = new SecTypeName(domainStr);
        final SecurityType signInType = SecurityType.get(storedDfltTypeName);
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
        return signInType;
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
