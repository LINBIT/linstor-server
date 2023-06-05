package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.security.Role;
import com.linbit.linstor.security.SecurityType;

/**
 * Database driver for {@link Role}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface SecRoleDatabaseDriver extends GenericDatabaseDriver<Role>
{
    SingleColumnDatabaseDriver<Role, SecurityType> getDomainDriver();

    SingleColumnDatabaseDriver<Role, Boolean> getRoleEnabledDriver();
}
