package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.k8s.K8sResourceClient;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_15_0;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_17_0;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorCrd;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorSpec;
import com.linbit.linstor.transaction.K8sCrdTransaction;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.databind.ObjectMapper;

@K8sCrdMigration(
    description = "change keys from base32 to sha256",
    version = 3
)
public class Migration_03_v1_17_0_ChangeKeysFromBase32ToSha256 extends BaseK8sCrdMigration
{
    public Migration_03_v1_17_0_ChangeKeysFromBase32ToSha256()
    {
        super(
            GenCrdV1_15_0.createMigrationContext(),
            GenCrdV1_17_0.createMigrationContext()
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public MigrationResult migrateImpl(MigrationContext migrationCtxRef) throws Exception
    {
        // We load entries from v1_15_0.ALL_TABLES, but will later write the corresponding v1_17_0 version of the same
        // table.
        HashMap<DatabaseTable, DatabaseTable> dbTableRemapping = getDbTableRemapping();

        K8sCrdTransaction txFrom = migrationCtxRef.txFrom;
        K8sCrdTransaction txTo = migrationCtxRef.txTo;

        // load data from database that needs to change
        HashMap<DatabaseTable, HashMap<String, LinstorCrd<LinstorSpec<?, ?>>>> loadedData = new HashMap<>();
        for (DatabaseTable dbTable : GenCrdV1_15_0.GeneratedDatabaseTables.ALL_TABLES)
        {
            HashMap<String, LinstorCrd<LinstorSpec<?, ?>>> crdMap;
            crdMap = (HashMap<String, LinstorCrd<LinstorSpec<?, ?>>>) txFrom.getCrd(dbTable);
            loadedData.put(dbTable, crdMap);
            /*
             * RollbackManager was BROKEN before this fix.
             * The problem was that the RollbackManager stored a map of arbitrary LinstorSpec (which is an interface)
             * references, but those implementations were missing the "@class" declaration which is required for
             * deserialization.
             *
             * The K8sClient api also handles every create request by unmarshalling the response from k8s. That means,
             * although the RollbackManager is now (hopefully) fixed, it cannot create any rollback instance because the
             * old (1.15.0) data are missing critical deserialization-information.
             */
            K8sResourceClient<LinstorCrd<LinstorSpec<?, ?>>> txFromClient;
            txFromClient = (K8sResourceClient<LinstorCrd<LinstorSpec<?, ?>>>) txFrom.getClient(dbTable);
            if (txFromClient != null)
            {
                /*
                 * we are iterating GeneratedDatabaseTables.ALL_TABLES which contains a table that the current version
                 * of the db / migration simply does not know. It should be save to ignore this case, since
                 * the migration will most likely not try to access a table it does not know?
                 */
                txFromClient.delete();
            }
        }

        // update CRD entries for all DatabaseTables
        updateCrdSchemaForAllTables();

        // write modified data to database
        ObjectMapper objMapper = new ObjectMapper();
        for (Entry<DatabaseTable, HashMap<String, LinstorCrd<LinstorSpec<?, ?>>>> oldEntries : loadedData.entrySet())
        {
            DatabaseTable dbTableOldVersion = oldEntries.getKey();
            DatabaseTable dbTableNewVersion = dbTableRemapping.get(dbTableOldVersion);
            Class<LinstorSpec<?, ?>> v1_17_0SpecClass = GenCrdV1_17_0.databaseTableToSpecClass(dbTableNewVersion);
            HashMap<String, LinstorCrd<LinstorSpec<?, ?>>> oldCrds = oldEntries.getValue();
            for (LinstorCrd<LinstorSpec<?, ?>> oldCrd : oldCrds.values())
            {
                // to make sure to use the correct constructors, we simply render the old spec to json and
                // let jackson parse that json into the new version/format
                String json = objMapper.writeValueAsString(oldCrd.getSpec());
                // however, the new json must include a "@class=..." entry, which is missing from the oldCrd.
                // that means we need to add that entry
                @SuppressWarnings("unchecked")
                Map<String, Object> map = objMapper.readValue(json, Map.class);

                // map.put("@c", v1_17_0SpecClass.getSimpleName());

                LinstorSpec<?, ?> v1_17_0_spec = objMapper.readValue(
                    objMapper.writeValueAsString(map),
                    v1_17_0SpecClass
                );

                // Use create here, as we previously removed all values from the DB.
                txTo.create(dbTableNewVersion, GenCrdV1_17_0.specToCrd(v1_17_0_spec));
            }
        }

        return null;
    }
}
