package com.linbit.linstor;

import com.linbit.SystemService;
import com.linbit.linstor.dbdrivers.DatabaseException;

/**
 * Database access for the linstor Controller module
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface ControllerDatabase extends SystemService
{
    int DEFAULT_TIMEOUT = 60000;

    void setTimeout(int timeout);

    void initializeDataSource(String dbConnectionUrl)
        throws DatabaseException;

    void migrate(String dbType);

    /**
     * Close all DB connections the calling thread had not closed yet.
     *
     * @return True if there was at least one open connection, false otherwise.
     */
    boolean closeAllThreadLocalConnections();

    @Override
    void shutdown();

    /**
     * Throws a DatabaseException if the database cannot be reached
     */
    void checkHealth() throws DatabaseException;
}
