package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.security.AccessControlEntry;
import com.linbit.linstor.security.AccessType;

/**
 * Database driver for {@link AccessControlEntry}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface SecObjProtAclDatabaseDriver extends GenericDatabaseDriver<AccessControlEntry>
{
    SingleColumnDatabaseDriver<AccessControlEntry, AccessType> getAccessTypeDriver();
}
