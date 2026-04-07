package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbcp.migration.Migration_2026_04_08_FixAutoBlockSizeProperty;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_33_1;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_33_1.PropsContainersSpec;

import java.util.Collection;

@K8sCrdMigration(
    description = "Fix Linstor/Drbd/auto-block-size property on controller level",
    version = 34
)
public class Migration_34_v1_33_1_FixAutoBlockSizeProperty extends BaseK8sCrdMigration
{
    public Migration_34_v1_33_1_FixAutoBlockSizeProperty()
    {
        super(
            GenCrdV1_33_1.createMigrationContext()
        );
    }

    @Override
    public @Nullable MigrationResult migrateImpl(MigrationContext ctxRef) throws Exception
    {
        Collection<GenCrdV1_33_1.PropsContainers> propCrds = ctxRef.txFrom.<GenCrdV1_33_1.PropsContainers, GenCrdV1_33_1.PropsContainersSpec>getCrd(
            GenCrdV1_33_1.GeneratedDatabaseTables.PROPS_CONTAINERS
        ).values();

        @Nullable GenCrdV1_33_1.PropsContainers oldCrd = null;
        @Nullable GenCrdV1_33_1.PropsContainers newCrd = null;

        for (GenCrdV1_33_1.PropsContainers propCrd : propCrds)
        {
            PropsContainersSpec propSpec = propCrd.getSpec();
            if (propSpec.propKey.equals(Migration_2026_04_08_FixAutoBlockSizeProperty.PROP))
            {
                if (propSpec.propsInstance.equals(Migration_2026_04_08_FixAutoBlockSizeProperty.CTRL))
                {
                    newCrd = propCrd;
                }
                else if (propSpec.propsInstance.equals(Migration_2026_04_08_FixAutoBlockSizeProperty.STLT))
                {
                    oldCrd = propCrd;
                }
            }
        }
        if (oldCrd != null)
        {
            ctxRef.txTo.delete(GenCrdV1_33_1.GeneratedDatabaseTables.PROPS_CONTAINERS, oldCrd);
            if (newCrd == null)
            {
                ctxRef.txTo.upsert(
                    GenCrdV1_33_1.GeneratedDatabaseTables.PROPS_CONTAINERS,
                    GenCrdV1_33_1.createPropsContainers(
                        Migration_2026_04_08_FixAutoBlockSizeProperty.CTRL,
                        Migration_2026_04_08_FixAutoBlockSizeProperty.PROP,
                        oldCrd.getSpec().propValue
                    )
                );
            }
        }

        return null;
    }
}
