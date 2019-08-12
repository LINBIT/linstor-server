package com.linbit.linstor.security;

import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.data.IdentityRoleEntry;
import com.linbit.linstor.security.data.SignInEntry;
import com.linbit.linstor.security.data.TypeEnforcementRule;

import java.util.List;

/**
 * Database interface for security objects persistence
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface DbAccessor
{
    SignInEntry getSignInEntry(ControllerDatabase ctrlDb, IdentityName idName)
        throws DatabaseException;
    IdentityRoleEntry getIdRoleMapEntry(ControllerDatabase ctrlDb, IdentityName idName, RoleName rlName)
        throws DatabaseException;
    IdentityRoleEntry getDefaultRole(ControllerDatabase ctrlDb, IdentityName idName)
        throws DatabaseException;

    List<String> loadIdentities(ControllerDatabase ctrlDb)
        throws DatabaseException;

    /**
     * Loads all security types and return them in a string list.
     * @param ctrlDb Controller database object from where to load the data.
     * @return List of security type strings
     * @throws DatabaseException
     */
    List<String> loadSecurityTypes(ControllerDatabase ctrlDb)
        throws DatabaseException;
    List<String> loadRoles(ControllerDatabase ctrlDb)
        throws DatabaseException;
    List<TypeEnforcementRule> loadTeRules(ControllerDatabase ctrlDb)
        throws DatabaseException;

    String loadSecurityLevel(ControllerDatabase ctrlDb)
        throws DatabaseException;
    boolean loadAuthRequired(ControllerDatabase ctrlDb)
        throws DatabaseException;
    void setSecurityLevel(ControllerDatabase ctrlDb, SecurityLevel newLevel)
        throws DatabaseException;
    void setAuthRequired(ControllerDatabase ctrlDb, boolean newPolicy)
        throws DatabaseException;
}
