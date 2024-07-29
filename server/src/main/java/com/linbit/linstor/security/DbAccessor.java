package com.linbit.linstor.security;

import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.pojo.IdentityRoleEntryPojo;
import com.linbit.linstor.security.pojo.SignInEntryPojo;

/**
 * Database interface for security objects persistence
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface DbAccessor<DB_TYPE extends ControllerDatabase>
{
    @Nullable
    SignInEntryPojo getSignInEntry(DB_TYPE ctrlDb, IdentityName idName)
        throws DatabaseException;

    void createSignInEntry(
        Identity idRef,
        Role dfltRoleRef,
        byte[] passwordSalt,
        byte[] passwordHash
    )
        throws DatabaseException;

    @Nullable
    IdentityRoleEntryPojo getIdRoleMapEntry(DB_TYPE ctrlDb, IdentityName idName, RoleName rlName)
        throws DatabaseException;

    IdentityRoleEntryPojo getDefaultRole(DB_TYPE ctrlDb, IdentityName idName)
        throws DatabaseException;

    void setSecurityLevel(DB_TYPE ctrlDb, SecurityLevel newLevel)
        throws DatabaseException;

    void setAuthRequired(DB_TYPE ctrlDb, boolean newPolicy)
        throws DatabaseException;
}
