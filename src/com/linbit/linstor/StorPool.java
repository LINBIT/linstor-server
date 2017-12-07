package com.linbit.linstor;

import java.sql.SQLException;
import java.util.Collection;
import java.util.UUID;

import com.linbit.TransactionObject;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.core.Satellite;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageDriver;
import com.linbit.linstor.storage.StorageException;

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
     * Returns the {@link com.linbit.linstor.StorPoolName}. This call is the same as <code>getDefinition(accCtx).getName()</code>
     * but does not require special access.
     */
    public StorPoolName getName();

    /**
     * Returns the {@link com.linbit.linstor.Node} this StorPool is associated with.
     */
    public Node getNode();

    /**
     * Returns the {@link com.linbit.linstor.StorPoolDefinition}.
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
    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException;

    /**
     * Registers the volume to this storPool
     */
    public void putVolume(AccessContext accCtx, Volume volume)
        throws AccessDeniedException;

    /**
     * Removes the volume from this storPool
     */
    public void removeVolume(AccessContext accCtx, Volume volume)
        throws AccessDeniedException;

    /**
     * Returns all currently registered volumes
     */
    public Collection<Volume> getVolumes(AccessContext accCtx)
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
