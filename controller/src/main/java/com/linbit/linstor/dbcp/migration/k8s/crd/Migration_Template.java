// package com.linbit.linstor.dbcp.migration.k8s.crd;
//
// import com.linbit.linstor.annotation.Nullable;
// import com.linbit.linstor.dbdrivers.DatabaseTable;
// import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV_OLD;
// import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV_NEW;
// import com.linbit.linstor.dbdrivers.k8s.crd.LinstorCrd;
// import com.linbit.linstor.dbdrivers.k8s.crd.LinstorSpec;
// import com.linbit.linstor.transaction.K8sCrdTransaction;
//
// import java.util.HashMap;
// import java.util.Map.Entry;
//
// import com.fasterxml.jackson.databind.ObjectMapper;
//
// /**
//  * K8s CRD Migration Template
//  *
//  * This template shows two common migration patterns:
//  *
//  * 1. SIMPLE MIGRATION (new table, no data transformation):
//  *    Just call updateCrdSchemaForAllTables() - see Migration_33_v1_33_1_AddAuthTokensTable.java
//  *
//  * 2. DATA TRANSFORMATION (modify existing data):
//  *    Load from old version, transform, write to new version - see example below
//  *    and Migration_28_v1_31_1_MoveTcpPortsToNodes.java for a complete implementation
//  */
// @K8sCrdMigration(
//     description = "Description of the migration",
//     version = -1  // Set to next sequential migration number
// )
// public class Migration_Template extends BaseK8sCrdMigration
// {
//     public Migration_Template()
//     {
//         super(
//             GenCrdV_OLD.createMigrationContext(),  // previous CRD version
//             GenCrdV_NEW.createMigrationContext()   // new CRD version (from make generate-db-constants)
//         );
//     }
//
//     @Override
//     public @Nullable MigrationResult migrateImpl(MigrationContext ctx) throws Exception
//     {
//         // ============================================================
//         // SIMPLE MIGRATION: Just adding new tables, no data changes
//         // ============================================================
//         // updateCrdSchemaForAllTables();
//         // return null;
//
//         // ============================================================
//         // DATA TRANSFORMATION: Migrating data from old to new format
//         // ============================================================
//
//         // Maps old version DatabaseTable references to new version references
//         HashMap<DatabaseTable, DatabaseTable> dbTableRemapping = getDbTableRemapping();
//
//         K8sCrdTransaction txFrom = ctx.txFrom;
//         K8sCrdTransaction txTo = ctx.txTo;
//
//         // 1. Load data from old version
//         HashMap<DatabaseTable, HashMap<String, LinstorCrd<LinstorSpec<?, ?>>>> loadedData = new HashMap<>();
//         for (DatabaseTable dbTable : GenCrdV_OLD.GeneratedDatabaseTables.ALL_TABLES)
//         {
//             @SuppressWarnings("unchecked")
//             HashMap<String, LinstorCrd<LinstorSpec<?, ?>>> crdMap =
//                 (HashMap<String, LinstorCrd<LinstorSpec<?, ?>>>) txFrom.getCrd(dbTable);
//             loadedData.put(dbTable, crdMap);
//
//             // Optional: delete old data if schema is incompatible
//             // txFrom.getClient(dbTable).delete();
//         }
//
//         // 2. Update CRD schema definitions
//         updateCrdSchemaForAllTables();
//
//         // 3. Transform and write data to new version
//         ObjectMapper objMapper = new ObjectMapper();
//         for (Entry<DatabaseTable, HashMap<String, LinstorCrd<LinstorSpec<?, ?>>>> entry : loadedData.entrySet())
//         {
//             DatabaseTable dbTableOld = entry.getKey();
//             DatabaseTable dbTableNew = dbTableRemapping.get(dbTableOld);
//
//             @SuppressWarnings("unchecked")
//             Class<LinstorSpec<?, ?>> newSpecClass =
//                 (Class<LinstorSpec<?, ?>>) GenCrdV_NEW.databaseTableToSpecClass(dbTableNew);
//
//             for (LinstorCrd<LinstorSpec<?, ?>> oldCrd : entry.getValue().values())
//             {
//                 // Convert old spec to JSON, then parse into new version
//                 String json = objMapper.writeValueAsString(oldCrd.getSpec());
//                 LinstorSpec<?, ?> newSpec = objMapper.readValue(json, newSpecClass);
//
//                 // Apply any data transformations here if needed
//                 // e.g., populate new columns, modify values, etc.
//
//                 txTo.create(dbTableNew, specToCrd(newSpec));
//             }
//         }
//
//         return null;
//     }
//
//     @SuppressWarnings("unchecked")
//     private <CRD extends LinstorCrd<SPEC>, SPEC extends LinstorSpec<CRD, SPEC>> LinstorCrd<SPEC> specToCrd(
//         LinstorSpec<?, ?> spec
//     )
//     {
//         return GenCrdV_NEW.specToCrd((SPEC) spec);
//     }
// }
