package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.security.Identity;

/**
 * Database driver for Security configurations.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface SecIdentityDatabaseDriver extends GenericDatabaseDriver<Identity>
{
    SingleColumnDatabaseDriver<Identity, byte[]> getPassHashDriver();

    SingleColumnDatabaseDriver<Identity, byte[]> getPassSaltDriver();

    SingleColumnDatabaseDriver<Identity, Boolean> getIdEnabledDriver();

    SingleColumnDatabaseDriver<Identity, Boolean> getIdLockedDriver();
}
