package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.ControllerK8sCrdDatabase;
import com.linbit.linstor.dbcp.migration.Migration_2025_01_29_SplitSnapPropsMoreFixes;
import com.linbit.linstor.dbcp.migration.Migration_2025_01_29_SplitSnapPropsMoreFixes.Changes;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_27_1;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_27_1.PropsContainers;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_27_1.PropsContainersSpec;

import static com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_27_1.GeneratedDatabaseTables.PROPS_CONTAINERS;

import java.sql.ResultSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

@K8sCrdMigration(
    description = "Auto-quorum rework",
    version = 26
)
public class Migration_26_v1_27_1_AutoQuorumInerhitRework extends BaseK8sCrdMigration
{
    public Migration_26_v1_27_1_AutoQuorumInerhitRework()
    {
        super(GenCrdV1_27_1.createMigrationContext());
    }

    @Override
    public MigrationResult migrateImpl(ControllerK8sCrdDatabase k8sDbRef) throws Exception
    {
        Collection<PropsContainers> crdList = txFrom.<PropsContainers, PropsContainersSpec>getCrd(
            PROPS_CONTAINERS
        ).values();

        for (PropsContainers propsCrd : crdList)
        {
            PropsContainersSpec propsSpec = propsCrd.getSpec();
            if (propsSpec.propKey.equalsIgnoreCase("DrbdOptions/auto-quorum"))
            {
                String autoQuorumValue = propsSpec.propValue;
                String propInstance = propsSpec.propsInstance;

                propInstance = propInstance.equalsIgnoreCase("/CTRL") ? "/STLT" : propInstance;

                if (autoQuorumValue.equalsIgnoreCase("disabled"))
                {
                    // if disabled set quorum -> off for the props_instance if not already set
                    txTo.create(PROPS_CONTAINERS,
                        GenCrdV1_27_1.createPropsContainers(
                            propInstance,
                            "Internal/Drbd/QuorumSetBy",
                            "user"
                        )
                    );

                    txTo.upsert(PROPS_CONTAINERS,
                        GenCrdV1_27_1.createPropsContainers(
                            propInstance,
                            "DrbdOptions/Resource/quorum",
                            "off"
                        )
                    );
                }
                else
                {
                    // if not disabled, set the current auto-quorum value to on-no-quorum
                    txTo.upsert(PROPS_CONTAINERS,
                        GenCrdV1_27_1.createPropsContainers(
                            propInstance,
                            "DrbdOptions/Resource/on-no-quorum",
                            autoQuorumValue
                        )
                    );
                }

                txTo.delete(PROPS_CONTAINERS, propsCrd);
            }
        }

        return null;
    }
}
