package com.linbit.linstor.dbdrivers;

import com.linbit.ImplementationError;
import com.linbit.linstor.dbdrivers.DatabaseDriverInfo.DatabaseType;
import com.linbit.linstor.dbdrivers.DatabaseTable.Column;
import com.linbit.utils.Base64;
import com.linbit.utils.ExceptionThrowingFunction;

import java.io.IOException;
import java.sql.JDBCType;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This class is basically only a wrapper for an Map<Column, Object].
 */
public class RawParameters
{
    private static final ObjectMapper OBJ_MAPPER = new ObjectMapper();

    private final DatabaseTable table;
    private final Map<String, Object> rawDataMap;
    private final DatabaseType dbType;

    public RawParameters(DatabaseTable tableRef, Map<String, Object> rawDataMapRef, DatabaseType dbTypeRef)
    {
        table = tableRef;
        rawDataMap = rawDataMapRef;
        dbType = dbTypeRef;
    }

    /**
     * ETCD always returns Strings for {@link #get(Column)}. This method however will call for ETCD the
     * {@link #getValueFromEtcd(Column)} before returning the value.
     * As a result, the value will be parsed based on the {@link Column#getSqlType()}.
     *
     * For non-ETCD engines, this method is a simple delegate for {@link #get(Column)}.
     */
    public <T> T getParsed(Column col)
    {
        T ret;
        switch (dbType)
        {
            case SQL:
            case K8S_CRD:
                ret = get(col);
                break;
            case ETCD:
                ret = getValueFromEtcd(col);
                break;
            default:
                throw new ImplementationError("Unknown db type: " + dbType);
        }
        return ret;
    }

    /**
     * This method does the same as {@link #build(Column, Class)}, but instead of depending on {@link #get(Column)}
     * this method calls {@link #getParsed(Column)}.
     */
    public <T, R, EXC extends Exception> R buildParsed(
        Column col,
        ExceptionThrowingFunction<T, R, EXC> func
    )
        throws EXC
    {
        T data = getParsed(col);
        R ret = null;
        if (data != null)
        {
            ret = func.accept(data);
        }
        return ret;
    }

    @SuppressWarnings("unchecked")
    private <T> T getValueFromEtcd(Column col) throws ImplementationError
    {
        T ret;
        String etcdVal = (String) rawDataMap.get(col.getName());
        if (etcdVal == null)
        {
            ret = null;
        }
        else
        {
            switch (col.getSqlType())
            {
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARBINARY:
                case Types.CLOB:
                    ret = (T) etcdVal;
                    break;
                case Types.BLOB:
                    ret = (T) Base64.decode(etcdVal);
                    break;
                case Types.BIT:
                case Types.BOOLEAN:
                    ret = (T) ((Boolean) Boolean.parseBoolean(etcdVal));
                    break;
                case Types.TINYINT:
                    ret = (T) ((Byte) Byte.parseByte(etcdVal));
                    break;
                case Types.SMALLINT:
                    ret = (T) ((Short) Short.parseShort(etcdVal));
                    break;
                case Types.INTEGER:
                    ret = (T) ((Integer) Integer.parseInt(etcdVal));
                    break;
                case Types.BIGINT:
                case Types.TIMESTAMP:
                    ret = (T) ((Long) Long.parseLong(etcdVal));
                    break;
                case Types.DATE:
                    ret = (T) (new Date(Long.parseLong(etcdVal)));
                    break;
                case Types.REAL:
                case Types.FLOAT:
                    ret = (T) ((Float) Float.parseFloat(etcdVal));
                    break;
                case Types.DOUBLE:
                    ret = (T) ((Double) Double.parseDouble(etcdVal));
                    break;
                default:
                    throw new ImplementationError("Unhandled SQL type: " + col.getSqlType());
            }
        }
        return ret;
    }

    @SuppressWarnings("unchecked")
    public <T> T get(Column col)
    {
        return (T) rawDataMap.get(col.getName());
    }

    public <T, R, EXC extends Exception> R build(
        Column col,
        ExceptionThrowingFunction<T, R, EXC> func
    )
        throws EXC
    {
        T data = get(col);
        R ret = null;
        if (data != null)
        {
            ret = func.accept(data);
        }
        return ret;
    }

    public <R extends Enum<R>> R build(Column col, Class<R> eType)
        throws IllegalArgumentException
    {
        String data = get(col);
        R ret = null;
        if (data != null)
        {
            ret = Enum.valueOf(eType, data);
        }
        return ret;
    }

    public List<String> getAsStringList(Column col) throws DatabaseException
    {
        List<String> ret;
        try
        {
            Object value = get(col);
            if (value == null)
            {
                ret = null;
            }
            else
            {
                int colSqlType = col.getSqlType();
                if (colSqlType == Types.VARCHAR || colSqlType == Types.CLOB)
                {
                    ret = new ArrayList<>(OBJ_MAPPER.readValue((String) value, List.class));
                }
                else if (colSqlType == Types.BLOB)
                {
                    ret = new ArrayList<>(OBJ_MAPPER.readValue((byte[]) value, List.class));
                }
                else
                {
                    throw new DatabaseException(
                        "Failed to deserialize json array. No handler found for sql type: " +
                            JDBCType.valueOf(colSqlType) +
                            " in table " + table.getName() + ", column " + col.getName()
                    );
                }
            }
        }
        catch (IOException exc)
        {
            throw new DatabaseException(
                "Failed to deserialize json array. Table: " + table.getName() + ", column: " + col.getName(),
                exc
            );
        }

        return ret;
    }

    public Short etcdGetShort(Column column)
    {
        return this.<String, Short, RuntimeException>build(column, Short::parseShort);
    }

    public Integer etcdGetInt(Column column)
    {
        return this.<String, Integer, RuntimeException>build(column, Integer::parseInt);
    }

    public Long etcdGetLong(Column column)
    {
        return this.<String, Long, RuntimeException>build(column, Long::parseLong);
    }

    public Boolean etcdGetBoolean(Column column)
    {
        return this.<String, Boolean, RuntimeException>build(column, Boolean::parseBoolean);
    }
}
