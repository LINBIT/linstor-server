package com.linbit.linstor.security;

import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.pojo.IdentityRoleEntryPojo;
import com.linbit.linstor.security.pojo.SignInEntryPojo;
import com.linbit.linstor.security.pojo.TypeEnforcementRulePojo;

import java.util.List;

/**
 * Database interface for security objects persistence
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface DbAccessor<DB_TYPE extends ControllerDatabase>
{
    SignInEntryPojo getSignInEntry(DB_TYPE ctrlDb, IdentityName idName)
        throws DatabaseException;

    void createSignInEntry(
        DB_TYPE         ctrlDb,
        IdentityName    idName,
        RoleName        dfltRlName,
        SecTypeName     dmnName,
        long            privileges,
        byte[]          password
    )
        throws DatabaseException;

    IdentityRoleEntryPojo getIdRoleMapEntry(DB_TYPE ctrlDb, IdentityName idName, RoleName rlName)
        throws DatabaseException;

    IdentityRoleEntryPojo getDefaultRole(DB_TYPE ctrlDb, IdentityName idName)
        throws DatabaseException;

    List<String> loadIdentities(DB_TYPE ctrlDb)
        throws DatabaseException;

    /**
     * Loads all security types and return them in a string list.
     * @param ctrlDb Controller database object from where to load the data.
     * @return List of security type strings
     * @throws DatabaseException
     */
    List<String> loadSecurityTypes(DB_TYPE ctrlDb)
        throws DatabaseException;

    List<String> loadRoles(DB_TYPE ctrlDb)
        throws DatabaseException;

    List<TypeEnforcementRulePojo> loadTeRules(DB_TYPE ctrlDb)
        throws DatabaseException;

    String loadSecurityLevel(DB_TYPE ctrlDb)
        throws DatabaseException;

    boolean loadAuthRequired(DB_TYPE ctrlDb)
        throws DatabaseException;

    void setSecurityLevel(DB_TYPE ctrlDb, SecurityLevel newLevel)
        throws DatabaseException;

    void setAuthRequired(DB_TYPE ctrlDb, boolean newPolicy)
        throws DatabaseException;
}
