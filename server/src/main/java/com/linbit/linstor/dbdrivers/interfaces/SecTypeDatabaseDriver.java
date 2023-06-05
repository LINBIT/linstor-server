package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.security.SecurityType;

/**
 * Database driver for {@link SecurityType}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface SecTypeDatabaseDriver extends GenericDatabaseDriver<SecurityType>
{
    SingleColumnDatabaseDriver<SecurityType, Boolean> getTypeEnabledDriver();
}
