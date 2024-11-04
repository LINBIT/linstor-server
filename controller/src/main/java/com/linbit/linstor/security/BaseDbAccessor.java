package com.linbit.linstor.security;

import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.SecConfigDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecConfigDatabaseDriver.SecConfigDbEntry;
import com.linbit.linstor.dbdrivers.interfaces.SecDefaultRoleDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecIdentityDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecIdentityDatabaseDriver.SecIdentityDbObj;
import com.linbit.utils.PairNonNull;

public abstract class BaseDbAccessor<CTRL_DB_TYPE extends ControllerDatabase> implements DbAccessor<CTRL_DB_TYPE>
{
    private final SecIdentityDatabaseDriver secIdDriver;
    private final SecConfigDatabaseDriver secCfgDriver;
    private final SecDefaultRoleDatabaseDriver secDfltRoleDriver;

    protected BaseDbAccessor(
        SecIdentityDatabaseDriver secIdDriverRef,
        SecConfigDatabaseDriver secCfgDriverRef,
        SecDefaultRoleDatabaseDriver secDfltRoleDriverRef
    )
    {
        secIdDriver = secIdDriverRef;
        secCfgDriver = secCfgDriverRef;
        secDfltRoleDriver = secDfltRoleDriverRef;
    }

    @Override
    public void createSignInEntry(
        Identity idRef,
        Role dfltRoleRef,
        byte[] passwordSaltRef,
        byte[] passwordHashRef
    )
        throws DatabaseException
    {
        secIdDriver.create(new SecIdentityDbObj(idRef, passwordHashRef, passwordSaltRef, true, false));
        if (dfltRoleRef != null)
        {
            secDfltRoleDriver.create(new PairNonNull<>(idRef, dfltRoleRef));
        }
    }

    @Override
    public void setSecurityLevel(ControllerDatabase ctrlDbRef, SecurityLevel newLevelRef) throws DatabaseException
    {
        secCfgDriver.getValueDriver()
            .update(
                new SecConfigDbEntry(SecurityDbConsts.KEY_DSP_SEC_LEVEL, newLevelRef.name()),
                null
            );
    }

    @Override
    public void setAuthRequired(ControllerDatabase ctrlDbRef, boolean newPolicyRef) throws DatabaseException
    {
        secCfgDriver.getValueDriver()
            .update(
                new SecConfigDbEntry(SecurityDbConsts.KEY_DSP_AUTH_REQ, Boolean.toString(newPolicyRef)),
                null
            );

    }
}
