package com.linbit.linstor;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Optional;
import java.util.UUID;

import com.linbit.fsevent.FileSystemWatch;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.core.Satellite;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageDriver;
import com.linbit.linstor.storage.StorageDriverKind;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.timer.CoreTimer;
import com.linbit.linstor.transaction.TransactionObject;

import java.util.List;
import java.util.Map;

/**
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface StorPool extends TransactionObject, DbgInstanceUuid
{
    /**
     * Returns the {@link UUID} of this object.
     */
    UUID getUuid();

    /**
     * Returns the {@link com.linbit.linstor.StorPoolName}. This call is the same as
     * <code>getDefinition(accCtx).getName()</code> but does not require special access.
     */
    StorPoolName getName();

    /**
     * Returns the {@link com.linbit.linstor.Node} this StorPool is associated with.
     */
    Node getNode();

    /**
     * Returns the {@link com.linbit.linstor.StorPoolDefinition}.
     */
    StorPoolDefinition getDefinition(AccessContext accCtx)
        throws AccessDeniedException;

    /**
     * Returns the {@link StorageDriver}.
     * Will return null on {@link Controller}, and non-null on {@link Satellite}.
     */
    StorageDriver getDriver(
        AccessContext accCtx,
        ErrorReporter errorReporter,
        FileSystemWatch fileSystemWatch,
        CoreTimer timer
    )
        throws AccessDeniedException;

    StorageDriverKind getDriverKind(AccessContext accCtx)
        throws AccessDeniedException;

    /**
     * Returns the simple class name of the used driver (not null on both, {@link Controller} and {@link Satellite})
     */
    String getDriverName();

    /**
     * Returns the configuration {@link Props}. This {@link Props} is also used to configure the {@link StorageDriver}.
     */
    Props getProps(AccessContext accCtx)
        throws AccessDeniedException;

    /**
     * Registers the volume to this storPool
     */
    void putVolume(AccessContext accCtx, Volume volume)
        throws AccessDeniedException;

    /**
     * Removes the volume from this storPool
     */
    void removeVolume(AccessContext accCtx, Volume volume)
        throws AccessDeniedException;

    /**
     * Returns all currently registered volumes
     */
    Collection<Volume> getVolumes(AccessContext accCtx)
        throws AccessDeniedException;

    /**
     * Takes all entries of the reserved namespace for the {@link StorageDriver}
     * from the config {@link Props} and calls
     * {@link StorageDriver#setConfiguration(java.util.Map)}
     * @throws StorageException
     */
    void reconfigureStorageDriver(StorageDriver storageDriver) throws StorageException;

    void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    StorPoolApi getApiData(AccessContext accCtx, Long fullSyncId, Long updateId)
        throws AccessDeniedException;

    long getFreeSpace(AccessContext accCtx) throws AccessDeniedException;

    interface StorPoolApi
    {
        UUID getStorPoolUuid();
        String getStorPoolName();
        UUID getStorPoolDfnUuid();
        String getNodeName();
        UUID getNodeUuid();
        String getDriver();
        Optional<Long> getFreeSpace();

        Map<String, String> getStorPoolProps();
        List<Volume.VlmApi> getVlmList();
        Map<String, String> getStorPoolStaticTraits();
    }

}
