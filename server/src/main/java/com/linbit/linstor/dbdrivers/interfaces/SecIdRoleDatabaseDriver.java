package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.security.Identity;
import com.linbit.linstor.security.Role;
import com.linbit.utils.PairNonNull;

/**
 * Database driver for {@link Role}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface SecIdRoleDatabaseDriver extends GenericDatabaseDriver<PairNonNull<Identity, Role>>
{
}
