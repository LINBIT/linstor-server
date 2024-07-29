package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.ControllerK8sCrdDatabase;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbcp.migration.etcd.Migration_33_CleanupOrphanedAcls;
import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.EbsRemotes;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.EbsRemotesSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.Files;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.FilesSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.KeyValueStore;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.KeyValueStoreSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.LinstorRemotes;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.LinstorRemotesSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.Nodes;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.NodesSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.ResourceDefinitions;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.ResourceDefinitionsSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.ResourceGroups;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.ResourceGroupsSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.Resources;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.ResourcesSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.S3Remotes;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.S3RemotesSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.Schedules;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.SchedulesSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.SecAclMap;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.SecAclMapSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.StorPoolDefinitions;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.StorPoolDefinitionsSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorCrd;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorSpec;

import java.util.Collection;
import java.util.HashSet;
import java.util.TreeSet;
import java.util.function.Function;

@K8sCrdMigration(
    description = "Cleanup orphaned ACL entries",
    version = 17
)
public class Migration_17_v1_19_1_CleanupOrphanedAcls extends BaseK8sCrdMigration
{
    public Migration_17_v1_19_1_CleanupOrphanedAcls()
    {
        super(GenCrdV1_19_1.createMigrationContext());
    }

    @Override
    public @Nullable MigrationResult migrateImpl(ControllerK8sCrdDatabase k8sDbRef) throws Exception
    {
        Collection<SecAclMap> crdList = txFrom.<SecAclMap, SecAclMapSpec>getCrd(
            GenCrdV1_19_1.GeneratedDatabaseTables.SEC_ACL_MAP
        ).values();

        TreeSet<String> neededObjPath = new TreeSet<>();
        neededObjPath.addAll(
            this.<Resources, ResourcesSpec>genericToObjPath(
                GenCrdV1_19_1.GeneratedDatabaseTables.RESOURCES,
                rsc -> Migration_33_CleanupOrphanedAcls.objPathRsc(rsc.nodeName, rsc.resourceName)
            )
        );
        neededObjPath.addAll(
            this.<ResourceDefinitions, ResourceDefinitionsSpec>genericToObjPath(
                GenCrdV1_19_1.GeneratedDatabaseTables.RESOURCE_DEFINITIONS,
                rscDfn -> Migration_33_CleanupOrphanedAcls.objPathRscDfn(rscDfn.resourceName)
            )
        );
        neededObjPath.addAll(
            this.<ResourceGroups, ResourceGroupsSpec>genericToObjPath(
                GenCrdV1_19_1.GeneratedDatabaseTables.RESOURCE_GROUPS,
                rscGrp -> Migration_33_CleanupOrphanedAcls.objPathRscGrp(rscGrp.resourceGroupName)
            )
        );
        neededObjPath.addAll(
            this.<Nodes, NodesSpec>genericToObjPath(
                GenCrdV1_19_1.GeneratedDatabaseTables.NODES,
                node -> Migration_33_CleanupOrphanedAcls.objPathNode(node.nodeName)
            )
        );
        neededObjPath.addAll(
            this.<StorPoolDefinitions, StorPoolDefinitionsSpec>genericToObjPath(
                GenCrdV1_19_1.GeneratedDatabaseTables.STOR_POOL_DEFINITIONS,
                spd -> Migration_33_CleanupOrphanedAcls.objPathStorPoolDfn(spd.poolName)
            )
        );
        neededObjPath.addAll(
            this.<KeyValueStore, KeyValueStoreSpec>genericToObjPath(
                GenCrdV1_19_1.GeneratedDatabaseTables.KEY_VALUE_STORE,
                kvs -> Migration_33_CleanupOrphanedAcls.objPathKvs(kvs.kvsName)
            )
        );
        neededObjPath.addAll(
            this.<Files, FilesSpec>genericToObjPath(
                GenCrdV1_19_1.GeneratedDatabaseTables.FILES,
                extFile -> Migration_33_CleanupOrphanedAcls.objPathExtFile(extFile.path)
            )
        );
        neededObjPath.addAll(
            this.<EbsRemotes, EbsRemotesSpec>genericToObjPath(
                GenCrdV1_19_1.GeneratedDatabaseTables.EBS_REMOTES,
                remote -> Migration_33_CleanupOrphanedAcls.objPathRemote(remote.name)
            )
        );
        neededObjPath.addAll(
            this.<LinstorRemotes, LinstorRemotesSpec>genericToObjPath(
                GenCrdV1_19_1.GeneratedDatabaseTables.LINSTOR_REMOTES,
                remote -> Migration_33_CleanupOrphanedAcls.objPathRemote(remote.name)
            )
        );
        neededObjPath.addAll(
            this.<S3Remotes, S3RemotesSpec>genericToObjPath(
                GenCrdV1_19_1.GeneratedDatabaseTables.S3_REMOTES,
                remote -> Migration_33_CleanupOrphanedAcls.objPathRemote(remote.name)
            )
        );
        neededObjPath.addAll(
            this.<Schedules, SchedulesSpec>genericToObjPath(
                GenCrdV1_19_1.GeneratedDatabaseTables.SCHEDULES,
                schedule -> Migration_33_CleanupOrphanedAcls.objPathSchedule(schedule.name)
            )
        );

        for (SecAclMap crd : crdList)
        {
            SecAclMapSpec spec = crd.getSpec();
            if (!Migration_33_CleanupOrphanedAcls.isObjPathNeeded(spec.objectPath, neededObjPath))
            {
                txTo.delete(GenCrdV1_19_1.GeneratedDatabaseTables.SEC_ACL_MAP, crd);
            }
        }
        return null;
    }

    private <CRD extends LinstorCrd<SPEC>, SPEC extends LinstorSpec<CRD, SPEC>> Collection<String> genericToObjPath(
        DatabaseTable dbTable,
        Function<SPEC, String> specToObjPathFuncRef
    )
    {
        HashSet<String> ret = new HashSet<>();
        Collection<CRD> crdList = txFrom.<CRD, SPEC>getCrd(dbTable).values();
        for (CRD crd : crdList)
        {
            ret.add(specToObjPathFuncRef.apply(crd.getSpec()));
        }
        return ret;
    }
}
