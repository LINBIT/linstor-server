// package com.linbit.linstor.dbcp.migration.k8s.crd;
//
// import com.linbit.ImplementationError;
// import com.linbit.linstor.ControllerK8sCrdDatabase;
// import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_15_0_RC_2;
// import com.linbit.linstor.transaction.BaseControllerK8sCrdTransactionMgr;
//
// import io.fabric8.kubernetes.client.KubernetesClient;
//
// @K8sCrdMigration(
// description = "",
// version = 1
// )
// public class Migration_Template extends BaseK8sCrdMigration
// {
// @Override
// public void migrate(ControllerK8sCrdDatabase k8sDbRef) throws Exception
// {
// BaseControllerK8sCrdTransactionMgr<GenCrdV1_15_0_RC_2.Rollback> txMgr = new BaseControllerK8sCrdTransactionMgr<>(
// k8sDbRef,
// GenCrdV1_15_0_RC_2.createTxMgrContext()
// );
//
// KubernetesClient k8sClient = k8sDbRef.getClient();
//
// // load data from database that needs to change
// // TODO implement loading data to migrate
//
// // update CRD entries for all DatabaseTables
// updateCrdSchemaForAllTables(k8sClient, GenCrdV1_15_0_RC_2::databaseTableToYamlLocation);
//
// // write modified data to database
// // TODO implement writing changed data
//
// throw new ImplementationError("Migration not implemented yet");
// }
// }
