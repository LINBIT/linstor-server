package com.linbit.linstor.dbdrivers;

import java.util.ArrayList;
import java.util.Map;

public interface ControllerDatabaseDriver<DATA, INIT_MAPS, LOAD_ALL>
{
    /**
     * Loads all entries from the database
     *
     * @param parentRef
     * @return
     * @throws DatabaseException
     */
    Map<DATA, INIT_MAPS> loadAll(LOAD_ALL parentRef) throws DatabaseException;

    /**
     * Returns an {@link ArrayList} of the keys returned by {@link #loadAll}
     *
     * @param loadAllData
     * @return
     * @throws DatabaseException
     */
    default ArrayList<DATA> loadAllAsList(LOAD_ALL loadAllData) throws DatabaseException
    {
        return new ArrayList<>(loadAll(loadAllData).keySet());
    }
}
