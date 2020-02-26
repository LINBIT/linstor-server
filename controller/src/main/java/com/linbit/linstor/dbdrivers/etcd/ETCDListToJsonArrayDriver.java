package com.linbit.linstor.dbdrivers.etcd;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseLoader;
import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.DatabaseTable.Column;
import com.linbit.linstor.dbdrivers.DbEngine.DataToString;
import com.linbit.linstor.dbdrivers.interfaces.updater.CollectionDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.manager.TransactionMgrETCD;
import com.linbit.utils.ExceptionThrowingFunction;
import com.linbit.utils.StringUtils;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

class ETCDListToJsonArrayDriver<DATA, LIST_TYPE> extends BaseEtcdDriver
    implements CollectionDatabaseDriver<DATA, LIST_TYPE>
{
    private static final ObjectMapper OBJ_MAPPER = new ObjectMapper();

    private final ErrorReporter errorReporter;
    private final Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> setters;
    private final Column columnToUpdate;
    private final DataToString<DATA> dataToString;

    private final DatabaseTable table;

    ETCDListToJsonArrayDriver(
        ErrorReporter errorReporterRef,
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> settersRef,
        Column columnToUpdateRef,
        DataToString<DATA> dataToStringRef,
        Provider<TransactionMgrETCD> transMgrProviderRef
    )
    {
        super(transMgrProviderRef);
        errorReporter = errorReporterRef;
        setters = settersRef;
        columnToUpdate = columnToUpdateRef;
        dataToString = dataToStringRef;

        table = columnToUpdateRef.getTable();
    }

    @Override
    public void insert(DATA parentRef, LIST_TYPE valueRef, Collection<LIST_TYPE> backingCollectionRef)
        throws DatabaseException
    {
        update(parentRef, valueRef, backingCollectionRef);
    }

    @Override
    public void remove(DATA parentRef, LIST_TYPE valueRef, Collection<LIST_TYPE> backingCollectionRef)
        throws DatabaseException
    {
        update(parentRef, valueRef, backingCollectionRef);
    }

    private void update(DATA data, LIST_TYPE ignored, Collection<LIST_TYPE> backingCollection)
        throws DatabaseException
    {
        try
        {
            String inlineId = dataToString.toString(data);
            errorReporter.logTrace(
                "Updating %s's %s to %s of %s",
                table.getName(),
                columnToUpdate.getName(),
                backingCollection.toString(),
                inlineId
            );
            namespace(table, getPrimaryKey(data, table))
                .put(
                    columnToUpdate,
                    OBJ_MAPPER.writeValueAsString(StringUtils.asStrList(backingCollection))
                );
        }
        catch (JsonProcessingException exc)
        {
            throw new DatabaseException(exc);
        }
        catch (AccessDeniedException exc)
        {
            DatabaseLoader.handleAccessDeniedException(exc);
        }
    }

    private String[] getPrimaryKey(DATA data, DatabaseTable tableRef) throws AccessDeniedException
    {
        List<String> pkList = new ArrayList<>();
        for (Column col : tableRef.values())
        {
            if (col.isPk())
            {
                pkList.add(Objects.toString(setters.get(col).accept(data)));
            }
        }

        String[] pkArray = new String[pkList.size()];
        pkList.toArray(pkArray);
        return pkArray;
    }

}
