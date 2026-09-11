package com.linbit.linstor.security;

import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.logging.ErrorReporter;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

import java.nio.charset.StandardCharsets;
import java.util.Hashtable;

/**
 * Validates user credentials against the LDAP server configured in the controller configuration.
 */
@Singleton
public class LdapAuthentication
{
    // Characters with special meaning in LDAP distinguished names or search filters.
    // User names containing any of these are rejected to prevent LDAP injection.
    private static final String LDAP_META_CHARACTERS = ",+\"\\<>;=#*()\0";

    private final ErrorReporter errorLog;
    private final CtrlConfig ctrlCfg;

    @Inject
    public LdapAuthentication(ErrorReporter errorLogRef, CtrlConfig ctrlCfgRef)
    {
        errorLog = errorLogRef;
        ctrlCfg = ctrlCfgRef;
    }

    /**
     * Authenticates the given user with a simple bind against the configured LDAP server,
     * optionally checking the configured search filter.
     *
     * @return the name of the authenticated user
     * @throws SignInException if the credentials are not valid or the LDAP server cannot be reached
     */
    public String authenticate(String user, byte[] password)
        throws SignInException
    {
        checkUserName(user);

        @SuppressWarnings("JdkObsolete") // InitialDirContext requires Hashtable
        Hashtable<String, String> ldapEnv = new Hashtable<>();
        ldapEnv.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        ldapEnv.put(Context.PROVIDER_URL, ctrlCfg.getLdapUri());
        ldapEnv.put(Context.SECURITY_AUTHENTICATION, "simple");
        String ldapDN = ctrlCfg.getLdapDn().replaceAll("\\{user}", user);
        ldapEnv.put(Context.SECURITY_PRINCIPAL, ldapDN);
        ldapEnv.put(Context.SECURITY_CREDENTIALS, new String(password, StandardCharsets.UTF_8));

        try
        {
            DirContext ctx = new InitialDirContext(ldapEnv);
            try
            {
                if (!ctrlCfg.getLdapSearchFilter().isEmpty())
                {
                    SearchControls searchControls = new SearchControls();
                    searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);

                    final String searchFilter = ctrlCfg.getLdapSearchFilter().replaceAll("\\{user}", user);

                    @SuppressWarnings("BanJNDI")
                    NamingEnumeration<SearchResult> result = ctx.search(
                        ctrlCfg.getLdapSearchBase(),
                        searchFilter,
                        searchControls
                    );
                    try
                    {
                        if (!result.hasMore())
                        {
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
                    }
                    finally
                    {
                        result.close();
                    }
                }

                errorLog.logInfo("LDAP User %s successfully authenticated.", user);
            }
            finally
            {
                ctx.close();
            }
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
        return user;
    }

    private void checkUserName(String user)
        throws InvalidCredentialsException
    {
        boolean valid = !user.isEmpty();
        for (int idx = 0; idx < user.length() && valid; idx++)
        {
            if (LDAP_META_CHARACTERS.indexOf(user.charAt(idx)) >= 0)
            {
                valid = false;
            }
        }
        if (!valid)
        {
            throw new InvalidCredentialsException(
                "Sign-in failed: Invalid user name",
                // Description
                "Sign-in failed",
                // Cause
                "The user name is empty or contains characters that are not allowed in LDAP names.",
                // Correction
                "The name of a valid identity and a matching password must be provided " +
                    "to sign in to the system.",
                null
            );
        }
    }
}
