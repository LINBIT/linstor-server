package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_25_1;
import com.linbit.linstor.transaction.K8sCrdTransaction;

import java.util.Collection;

@K8sCrdMigration(
    description = "Drop OpenFlex tables",
    version = 18
)
public class Migration_18_v1_25_1_DropOpenflexTables extends BaseK8sCrdMigration
{
    public Migration_18_v1_25_1_DropOpenflexTables()
    {
        super(
            GenCrdV1_19_1.createMigrationContext(),
            GenCrdV1_25_1.createMigrationContext()
        );
    }

    @Override
    public @Nullable MigrationResult migrateImpl(MigrationContext migrationCtxRef) throws Exception
    {
        K8sCrdTransaction txFrom = migrationCtxRef.txFrom;

        Collection<GenCrdV1_19_1.LayerOpenflexResourceDefinitions> ofRdList = txFrom.<GenCrdV1_19_1.LayerOpenflexResourceDefinitions, GenCrdV1_19_1.LayerOpenflexResourceDefinitionsSpec>getCrd(
            GenCrdV1_19_1.GeneratedDatabaseTables.LAYER_OPENFLEX_RESOURCE_DEFINITIONS
        ).values();

        for (GenCrdV1_19_1.LayerOpenflexResourceDefinitions ofRd : ofRdList)
        {
            txFrom.delete(GenCrdV1_19_1.GeneratedDatabaseTables.LAYER_OPENFLEX_RESOURCE_DEFINITIONS, ofRd);
        }

        Collection<GenCrdV1_19_1.LayerOpenflexVolumes> ofVlmList = txFrom.<GenCrdV1_19_1.LayerOpenflexVolumes, GenCrdV1_19_1.LayerOpenflexVolumesSpec>getCrd(
            GenCrdV1_19_1.GeneratedDatabaseTables.LAYER_OPENFLEX_VOLUMES
        ).values();

        for (GenCrdV1_19_1.LayerOpenflexVolumes ofVlm : ofVlmList)
        {
            txFrom.delete(GenCrdV1_19_1.GeneratedDatabaseTables.LAYER_OPENFLEX_VOLUMES, ofVlm);
        }

        // update CRD entries for all DatabaseTables
        updateCrdSchemaForAllTables();

        return null;
    }
}
