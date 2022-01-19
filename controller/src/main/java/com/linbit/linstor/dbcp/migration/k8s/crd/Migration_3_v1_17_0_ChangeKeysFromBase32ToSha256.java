package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_15_0;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_17_0;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorCrd;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorSpec;

import java.util.HashMap;
import java.util.Map.Entry;

import com.fasterxml.jackson.databind.ObjectMapper;

@K8sCrdMigration(
    description = "change keys from base32 to sha256",
    version = 3
)
public class Migration_3_v1_17_0_ChangeKeysFromBase32ToSha256 extends BaseK8sCrdMigration
{
    public Migration_3_v1_17_0_ChangeKeysFromBase32ToSha256()
    {
        super(
            GenCrdV1_15_0.createTxMgrContext(),
            GenCrdV1_17_0.createTxMgrContext(),
            GenCrdV1_17_0.createSchemaUpdateContext()
        );
    }

    @Override
    public MigrationResult migrateImpl() throws Exception
    {
        // load data from database that needs to change
        HashMap<DatabaseTable, HashMap<String, LinstorCrd<LinstorSpec>>> loadedData = new HashMap<>();
        for (DatabaseTable dbTable : GeneratedDatabaseTables.ALL_TABLES)
        {
            HashMap<String, LinstorCrd<LinstorSpec>> crdMap = txFrom.getCrd(dbTable);
            loadedData.put(dbTable, crdMap);
            for (LinstorCrd<LinstorSpec> loadedCrd : crdMap.values())
            {
                txFrom.delete(dbTable, loadedCrd);
            }
        }

        // update CRD entries for all DatabaseTables
        updateCrdSchemaForAllTables();

        // write modified data to database
        ObjectMapper objMapper = new ObjectMapper();
        for (Entry<DatabaseTable, HashMap<String, LinstorCrd<LinstorSpec>>> oldEntries : loadedData.entrySet())
        {
            DatabaseTable dbTable = oldEntries.getKey();
            Class<LinstorSpec> v1_17_0SpecClass = GenCrdV1_17_0.databaseTableToSpecClass(dbTable);
            HashMap<String, LinstorCrd<LinstorSpec>> oldCrds = oldEntries.getValue();
            for (LinstorCrd<LinstorSpec> oldCrd : oldCrds.values())
            {
                // to make sure to use the correct constructors, we simply render the old spec to json and
                // let jackson parse that json into the new version/format
                String json = objMapper.writeValueAsString(oldCrd.getSpec());
                LinstorSpec v1_17_0_spec = objMapper.readValue(json, v1_17_0SpecClass);
                txTo.update(dbTable, GenCrdV1_17_0.specToCrd(v1_17_0_spec));
            }
        }

        MigrationResult result = new MigrationResult();
        result.setForceFromTxCommit(true);
        return result;
    }
}
