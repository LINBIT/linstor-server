package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.security.Identity;
import com.linbit.linstor.security.Role;
import com.linbit.utils.PairNonNull;

/**
 * Database driver for Security configurations.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface SecDefaultRoleDatabaseDriver extends GenericDatabaseDriver<PairNonNull<Identity, Role>>
{
}
