package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;

/**
 * Database driver for {@link com.linbit.linstor.core.objects.StorPoolDefinition}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface StorPoolDefinitionDatabaseDriver extends GenericDatabaseDriver<StorPoolDefinition>
{
    StorPoolDefinition createDefaultDisklessStorPool() throws DatabaseException;
}
