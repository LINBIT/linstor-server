package com.linbit.linstor.dbdrivers.interfaces;

import java.sql.SQLException;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeConnectionData;

/**
 * Database driver for {@link com.linbit.linstor.VolumeConnectionData}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface VolumeConnectionDataDatabaseDriver
{
    /**
     * Loads the {@link com.linbit.linstor.VolumeConnectionData} specified by the parameters
     * {@code resourceName}, {@code sourceNodeName} and {@code targetNodeName}.
     * <br>
     * By convention the {@link NodeName} of @{@code sourceVolume} has to be
     * alphanumerically smaller than the {@link NodeName} of {@code targetVolume}
     * @param sourceVolume
     *  Part of the primary key specifying the database entry
     * @param targetVolume
     *  Part of the primary key specifying the database entry
     * @param logWarnIfNotExists
     *  If true a warning is logged if the requested entry does not exist
     *
     * @throws SQLException
     */
    VolumeConnectionData load(Volume sourceVolume, Volume targetVolume, boolean logWarnIfNotExists)
        throws SQLException;

    /**
     * Persists the given {@link com.linbit.linstor.VolumeConnectionData} into the database.
     *
     * @param conDfnData
     *  The data to be stored (including the primary key)
     *
     * @throws SQLException
     */
    void create(VolumeConnectionData conDfnData) throws SQLException;

    /**
     * Removes the given {@link com.linbit.linstor.VolumeConnectionData} from the database
     *
     * @param conDfnData
     *  The data identifying the database entry to delete
     *
     * @throws SQLException
     */
    void delete(VolumeConnectionData conDfnData) throws SQLException;
}
