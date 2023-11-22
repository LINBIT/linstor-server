package com.linbit.linstor.transaction;

public class K8sCrdMigrationContext
{
    public final BaseControllerK8sCrdTransactionMgrContext txMgrContext;
    public final K8sCrdSchemaUpdateContext schemaCtx;

    public K8sCrdMigrationContext(
        BaseControllerK8sCrdTransactionMgrContext txMgrContextRef,
        K8sCrdSchemaUpdateContext schemaCtxRef
    )
    {
        txMgrContext = txMgrContextRef;
        schemaCtx = schemaCtxRef;
    }
}
