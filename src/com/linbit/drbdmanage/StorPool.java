package com.linbit.drbdmanage;

import java.sql.SQLException;
import java.util.UUID;

import com.linbit.TransactionObject;
import com.linbit.drbdmanage.core.Controller;
import com.linbit.drbdmanage.core.Satellite;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.ObjectProtection;
import com.linbit.drbdmanage.storage.StorageDriver;
import com.linbit.drbdmanage.storage.StorageException;

/**
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface StorPool extends TransactionObject
{
    public static final String STORAGE_DRIVER_PROP_NAMESPACE = "storDriver";

    /**
     * Returns the {@link UUID} of this object.
     */
    public UUID getUuid();

    /**
     * Returns the {@link StorPoolName}. This call is the same as <code>getDefinition(accCtx).getName()</code>
     * but does not require special access.
     */
    public StorPoolName getName();

    /**
     * Returns the {@link ObjectProtection}
     */
    public ObjectProtection getObjProt();

    /**
     * Returns the {@link StorPoolDefinition}.
     */
    public StorPoolDefinition getDefinition(AccessContext accCtx)
        throws AccessDeniedException;

    /**
     * Returns the {@link StorageDriver}.
     * Will return null on {@link Controller}, and non-null on {@link Satellite}.
     */
    public StorageDriver getDriver(AccessContext accCtx)
        throws AccessDeniedException;

    /**
     * Returns the simple class name of the used driver (not null on both, {@link Controller} and {@link Satellite})
     */
    public String getDriverName();

    /**
     * Returns the configuration {@link Props}. This {@link Props} is also used to configure the {@link StorageDriver}.
     */
    public Props getConfiguration(AccessContext accCtx)
        throws AccessDeniedException;

    /**
     * Takes all entries of the reserved namespace for the {@link StorageDriver} from the config {@link Props} and calls
     * {@link StorageDriver#setConfiguration(java.util.Map)}
     * @throws StorageException
     */
    public void reconfigureStorageDriver() throws StorageException;

    public void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException;
}
