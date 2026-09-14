package com.linbit.linstor.core.apicallhandler.controller.db;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.dbcp.k8s.crd.DbK8sCrd;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseDriverInfo.DatabaseType;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.DatabaseTable.Column;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent.ResourceDefinitionsSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorSpec;
import com.linbit.linstor.testutils.EmptyErrorReporter;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.transaction.manager.TransactionMgrGenerator;
import com.linbit.linstor.transaction.manager.TransactionMgrSQL;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.mockito.InOrder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests the reordering of self-referencing tables (currently only RESOURCE_DEFINITIONS, where a
 * snapshot-definition references its resource-definition via PARENT_UUID -> UUID) so that referenced entries
 * are always imported before the entries referencing them.
 */
public class DbExportImportHelperTest
{
    private static final String RD_A_UUID = "aaaaaaaa-0000-0000-0000-000000000000";
    private static final String RD_B_UUID = "bbbbbbbb-0000-0000-0000-000000000000";
    private static final String SNAP_1_UUID = "11111111-0000-0000-0000-000000000000";
    private static final String SNAP_2_UUID = "22222222-0000-0000-0000-000000000000";

    @Test
    public void snapDfnBeforeitsRscDfnGetsReordered()
    {
        DbExportPojoData.Table tbl = rscDfnTable(
            snapDfn(SNAP_1_UUID, "rsc-a", "snap1", RD_A_UUID),
            rscDfn(RD_A_UUID, "rsc-a")
        );

        DbExportImportHelper.reorderSelfReferencingEntries(tbl, GeneratedDatabaseTables.SELF_REFERENCING_FOREIGN_KEYS);

        assertThat(uuids(tbl)).containsExactly(RD_A_UUID, SNAP_1_UUID);
    }

    @Test
    public void alreadyValidOrderIsKept()
    {
        DbExportPojoData.Table tbl = rscDfnTable(
            rscDfn(RD_A_UUID, "rsc-a"),
            snapDfn(SNAP_1_UUID, "rsc-a", "snap1", RD_A_UUID),
            rscDfn(RD_B_UUID, "rsc-b"),
            snapDfn(SNAP_2_UUID, "rsc-b", "snap2", RD_B_UUID)
        );

        DbExportImportHelper.reorderSelfReferencingEntries(tbl, GeneratedDatabaseTables.SELF_REFERENCING_FOREIGN_KEYS);

        assertThat(uuids(tbl)).containsExactly(RD_A_UUID, SNAP_1_UUID, RD_B_UUID, SNAP_2_UUID);
    }

    @Test
    public void interleavedSnapDfnsEndUpAfterTheirRscDfns()
    {
        DbExportPojoData.Table tbl = rscDfnTable(
            snapDfn(SNAP_1_UUID, "rsc-a", "snap1", RD_A_UUID),
            rscDfn(RD_B_UUID, "rsc-b"),
            snapDfn(SNAP_2_UUID, "rsc-b", "snap2", RD_B_UUID),
            rscDfn(RD_A_UUID, "rsc-a")
        );

        DbExportImportHelper.reorderSelfReferencingEntries(tbl, GeneratedDatabaseTables.SELF_REFERENCING_FOREIGN_KEYS);

        List<String> uuids = uuids(tbl);
        assertThat(uuids).containsExactlyInAnyOrder(RD_A_UUID, RD_B_UUID, SNAP_1_UUID, SNAP_2_UUID);
        assertThat(uuids.indexOf(SNAP_1_UUID)).isGreaterThan(uuids.indexOf(RD_A_UUID));
        assertThat(uuids.indexOf(SNAP_2_UUID)).isGreaterThan(uuids.indexOf(RD_B_UUID));
    }

    @Test
    public void danglingParentReferenceIsNotDropped()
    {
        DbExportPojoData.Table tbl = rscDfnTable(
            snapDfn(SNAP_1_UUID, "rsc-a", "snap1", "deadbeef-0000-0000-0000-000000000000"),
            rscDfn(RD_A_UUID, "rsc-a")
        );

        DbExportImportHelper.reorderSelfReferencingEntries(tbl, GeneratedDatabaseTables.SELF_REFERENCING_FOREIGN_KEYS);

        assertThat(uuids(tbl)).containsExactlyInAnyOrder(SNAP_1_UUID, RD_A_UUID);
    }

    @Test
    public void exportWithoutParentUuidColumnStaysUntouched()
    {
        List<DbExportPojoData.Column> clmsWithoutParentUuid = new ArrayList<>();
        for (Column clm : GeneratedDatabaseTables.RESOURCE_DEFINITIONS.values())
        {
            if (!clm.getName().equals(GeneratedDatabaseTables.ResourceDefinitions.PARENT_UUID.getName()))
            {
                clmsWithoutParentUuid.add(
                    new DbExportPojoData.Column(clm.getName(), clm.getSqlType(), clm.isPk(), clm.isNullable())
                );
            }
        }
        DbExportPojoData.Table tbl = new DbExportPojoData.Table(
            GeneratedDatabaseTables.RESOURCE_DEFINITIONS.getName(),
            clmsWithoutParentUuid,
            new ArrayList<>(
                Arrays.asList(
                    snapDfn(SNAP_1_UUID, "rsc-a", "snap1", RD_A_UUID),
                    rscDfn(RD_A_UUID, "rsc-a")
                )
            ),
            null
        );

        DbExportImportHelper.reorderSelfReferencingEntries(tbl, GeneratedDatabaseTables.SELF_REFERENCING_FOREIGN_KEYS);

        assertThat(uuids(tbl)).containsExactly(SNAP_1_UUID, RD_A_UUID);
    }

    @Test
    public void otherTablesStayUntouched()
    {
        DbExportPojoData.Table tbl = new DbExportPojoData.Table(
            GeneratedDatabaseTables.NODES.getName(),
            new ArrayList<>(),
            new ArrayList<>(
                Arrays.asList(
                    snapDfn(SNAP_1_UUID, "rsc-a", "snap1", RD_A_UUID),
                    rscDfn(RD_A_UUID, "rsc-a")
                )
            ),
            null
        );

        DbExportImportHelper.reorderSelfReferencingEntries(tbl, GeneratedDatabaseTables.SELF_REFERENCING_FOREIGN_KEYS);

        assertThat(uuids(tbl)).containsExactly(SNAP_1_UUID, RD_A_UUID);
    }

    @Test
    public void cyclicReferencesAreKeptInsteadOfDropped()
    {
        // cannot occur with real data, but a malformed export must not lose entries silently
        DbExportPojoData.Table tbl = rscDfnTable(
            snapDfn(SNAP_1_UUID, "rsc-a", "snap1", SNAP_2_UUID),
            snapDfn(SNAP_2_UUID, "rsc-a", "snap2", SNAP_1_UUID),
            rscDfn(RD_A_UUID, "rsc-a")
        );

        DbExportImportHelper.reorderSelfReferencingEntries(tbl, GeneratedDatabaseTables.SELF_REFERENCING_FOREIGN_KEYS);

        assertThat(uuids(tbl)).containsExactlyInAnyOrder(RD_A_UUID, SNAP_1_UUID, SNAP_2_UUID);
    }

    private DbExportPojoData.Table rscDfnTable(LinstorSpec<?, ?>... specs)
    {
        List<DbExportPojoData.Column> clmDescrList = new ArrayList<>();
        for (Column clm : GeneratedDatabaseTables.RESOURCE_DEFINITIONS.values())
        {
            clmDescrList.add(
                new DbExportPojoData.Column(clm.getName(), clm.getSqlType(), clm.isPk(), clm.isNullable())
            );
        }
        return new DbExportPojoData.Table(
            GeneratedDatabaseTables.RESOURCE_DEFINITIONS.getName(),
            clmDescrList,
            new ArrayList<>(Arrays.asList(specs)),
            null
        );
    }

    private LinstorSpec<?, ?> rscDfn(String uuid, String rscName)
    {
        return rscDfnSpec(uuid, rscName, "", null);
    }

    private LinstorSpec<?, ?> snapDfn(String uuid, String rscName, String snapName, String parentUuid)
    {
        return rscDfnSpec(uuid, rscName, snapName, parentUuid);
    }

    private LinstorSpec<?, ?> rscDfnSpec(
        String uuid,
        String rscName,
        String snapName,
        @Nullable String parentUuid
    )
    {
        return new ResourceDefinitionsSpec(
            uuid,
            rscName.toUpperCase(),
            snapName.toUpperCase(),
            rscName,
            snapName,
            0L,
            "",
            null,
            "DfltRscGrp",
            parentUuid
        );
    }

    private List<String> uuids(DbExportPojoData.Table tblRef)
    {
        List<String> ret = new ArrayList<>();
        for (LinstorSpec<?, ?> spec : tblRef.data)
        {
            ret.add((String) spec.getByColumn(GeneratedDatabaseTables.ResourceDefinitions.UUID.getName()));
        }
        return ret;
    }

    /**
     * Regression test: when {@link DbExportImportHelper#exportDb()} has to start its own transaction (i.e. the scope
     * was not seeded with one, as is the case for the AutoDbExportTask), the connection of that transaction must be
     * returned to the pool afterwards. Otherwise every export leaks one connection, which eventually exhausts the
     * connection pool and blocks all further database operations.
     */
    @Test
    public void exportDbReturnsSelfStartedTransactionConnection() throws Exception
    {
        TransactionMgr txMgr = mock(TransactionMgrSQL.class);
        DbExportImportHelper helper = createHelper(txMgr, new HashMap<>());

        helper.exportDb();

        InOrder inOrder = inOrder(txMgr);
        inOrder.verify(txMgr).rollback();
        inOrder.verify(txMgr).returnConnection();
    }

    @Test
    public void exportDbReturnsSelfStartedTransactionConnectionOnError() throws Exception
    {
        TransactionMgr txMgr = mock(TransactionMgrSQL.class);
        @SuppressWarnings("unchecked")
        AbsDatabaseDriver<?, ?, ?> failingDriver = mock(AbsDatabaseDriver.class);
        when(failingDriver.export()).thenThrow(new DatabaseException("test"));
        Map<DatabaseTable, AbsDatabaseDriver<?, ?, ?>> drivers = new HashMap<>();
        drivers.put(GeneratedDatabaseTables.ALL_TABLES[0], failingDriver);
        DbExportImportHelper helper = createHelper(txMgr, drivers);

        assertThatThrownBy(helper::exportDb).isInstanceOf(ApiDatabaseException.class);

        InOrder inOrder = inOrder(txMgr);
        inOrder.verify(txMgr).rollback();
        inOrder.verify(txMgr).returnConnection();
    }

    @Test
    public void exportDbDoesNotTouchForeignTransaction() throws Exception
    {
        TransactionMgr txMgr = mock(TransactionMgrSQL.class);
        LinStorScope scope = mock(LinStorScope.class);
        when(scope.isSeeded(any())).thenReturn(true); // transaction was started by the caller
        DbExportImportHelper helper = createHelper(txMgr, new HashMap<>(), scope);

        helper.exportDb();

        verify(txMgr, never()).rollback();
        verify(txMgr, never()).returnConnection();
    }

    private DbExportImportHelper createHelper(
        TransactionMgr txMgr,
        Map<DatabaseTable, AbsDatabaseDriver<?, ?, ?>> drivers
    )
    {
        LinStorScope scope = mock(LinStorScope.class);
        when(scope.isSeeded(any())).thenReturn(false);
        return createHelper(txMgr, drivers, scope);
    }

    private DbExportImportHelper createHelper(
        TransactionMgr txMgr,
        Map<DatabaseTable, AbsDatabaseDriver<?, ?, ?>> drivers,
        LinStorScope scope
    )
    {
        TransactionMgrGenerator txMgrGenerator = mock(TransactionMgrGenerator.class);
        when(txMgrGenerator.startTransaction()).thenReturn(txMgr);

        DbEngine dbEngine = mock(DbEngine.class);
        when(dbEngine.getType()).thenReturn(DatabaseType.SQL);

        CtrlConfig ctrlCfg = mock(CtrlConfig.class);
        when(ctrlCfg.getDbConnectionUrl()).thenReturn("jdbc:h2:mem:test");

        LockGuardFactory lockGuardFactory = mock(LockGuardFactory.class);
        when(lockGuardFactory.build(any(LockType.class), any(LockObj[].class))).thenReturn(mock(LockGuard.class));

        return new DbExportImportHelper(
            new EmptyErrorReporter(),
            drivers,
            dbEngine,
            mock(DbConnectionPool.class),
            mock(DbK8sCrd.class),
            () -> txMgrGenerator,
            () -> txMgr,
            scope,
            lockGuardFactory,
            ctrlCfg
        );
    }
}
