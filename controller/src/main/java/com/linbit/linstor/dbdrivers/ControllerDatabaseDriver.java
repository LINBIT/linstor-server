package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorSpec;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public interface ControllerDatabaseDriver<DATA, INIT_MAPS, LOAD_ALL>
{
    DatabaseTable getDbTable();

    List<LinstorSpec<?, ?>> export() throws DatabaseException;

    /**
     * Loads all entries from the database
     *
     * @param parentRef
     * @return
     * @throws DatabaseException
     */
    Map<DATA, INIT_MAPS> loadAll(@Nullable LOAD_ALL parentRef) throws DatabaseException;

    /**
     * Returns an {@link ArrayList} of the keys returned by {@link #loadAll}
     *
     * @param loadAllData
     * @return
     * @throws DatabaseException
     */
    default ArrayList<DATA> loadAllAsList(@Nullable LOAD_ALL loadAllData) throws DatabaseException
    {
        return new ArrayList<>(loadAll(loadAllData).keySet());
    }
}
