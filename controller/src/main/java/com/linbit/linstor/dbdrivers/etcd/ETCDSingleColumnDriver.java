package com.linbit.linstor.dbdrivers.etcd;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseTable.Column;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.transaction.manager.TransactionMgrETCD;

import javax.inject.Provider;

import java.util.function.Function;

public class ETCDSingleColumnDriver<PARENT, COL_VALUE> extends BaseEtcdDriver
    implements SingleColumnDatabaseDriver<PARENT, COL_VALUE>
{
    private final Function<PARENT, String[]> primaryKeyGetter;
    private final Column col;
    private final Function<COL_VALUE, String> toStringFkt;

    public ETCDSingleColumnDriver(
        Provider<TransactionMgrETCD> transMgrProviderRef,
        Column colRef,
        Function<PARENT, String[]> primaryKeyGetterRef,
        Function<COL_VALUE, String> toStringFktRef
    )
    {
        super(transMgrProviderRef);
        col = colRef;
        primaryKeyGetter = primaryKeyGetterRef;
        toStringFkt = toStringFktRef;
    }

    @Override
    public void update(PARENT parentRef, COL_VALUE elementRef) throws DatabaseException
    {
        namespace(col.getTable(), primaryKeyGetter.apply(parentRef))
            .put(col, toStringFkt.apply(elementRef));
    }

}
