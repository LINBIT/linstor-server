package com.linbit.linstor.dbdrivers;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nonnull;
import com.linbit.linstor.annotation.Nullable;
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
import java.util.TreeMap;
import java.util.function.UnaryOperator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This class is basically only a wrapper for an Map<Column, Object].
 */
public class RawParameters
{
    private static final ObjectMapper OBJ_MAPPER = new ObjectMapper();

    private final @Nullable DatabaseTable table;
    private final Map<String, Object> rawDataMap;
    private final DatabaseType dbType;

    public RawParameters(@Nullable DatabaseTable tableRef, Map<String, Object> rawDataMapRef, DatabaseType dbTypeRef)
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
    public <T> @Nullable T getParsed(Column col)
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
    /*
     * yes, these annotations look weird, but they are correct:
     * T can be null whithin this method, but before it is passed to func a null-check is performed, so that func can
     * expect a @Nonnull T
     */
    public <@Nullable T, @Nullable R, EXC extends Exception> @Nullable R buildParsed(
        Column col,
        ExceptionThrowingFunction<@Nonnull T, R, EXC> func
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
    private <T> @Nullable T getValueFromEtcd(Column col) throws ImplementationError
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
    public <T> @Nullable T get(Column col)
    {
        return (T) rawDataMap.get(col.getName());
    }

    public <T, @Nullable R, EXC extends Exception> @Nullable R build(
        Column col,
        ExceptionThrowingFunction<@Nonnull T, R, EXC> func
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

    public <@Nullable R extends Enum<R>> @Nullable R build(Column col, Class<R> eType)
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


    public @Nullable List<String> getAsStringList(Column col) throws DatabaseException
    {
        // Make sure to actually use generic types instead of just "new TypeReference<>()" to ensure type-safety.
        // Otherwise you end up with an Object being just casted to List<String>. The problem with that is that a
        // List<Integer> would be read as such by Jackson, casted to an Object (due to the missing generic types) and
        // only when this method returns the Object will be cased to the desired type. That cast will *NOT* cause a
        // ClassCastException, but the first attempt to actually use one of the generic types (in case of a List<String>
        // something like "String bla = list.get(0);").
        return getFromJson(col, new TypeReference<List<String>>()
        {
        }, ArrayList::new);
    }

    public @Nullable Map<String, Integer> getAsStringIntegerMap(Column col) throws DatabaseException
    {
        // Make sure to actually use generic types instead of just "new TypeReference<>()" to ensure type-safety.
        // Otherwise you end up with a Map<Object, Object> being just casted
        return getFromJson(col, new TypeReference<Map<String, Integer>>()
        {
        }, TreeMap::new);
    }

    /**
     * Reads the specified column either as String or as byte[] (depending on the column's SQL type), gives the content
     * to Jackson to read and the <code>typeRef</code> dependent result is optionally converted.
     *
     * @param <T>
     * @param col
     * @param typeRef
     *     Make sure to either use proper generic types when creating a new {@link TypeReference} or Java can surely
     *     figure the generic type out on its own, without having to default to <code>Object</code> (aka using
     *     type-erasure). An type-erased <code>new TypeReference<>(){}</code> would only create a
     *     <code>new TypeReference&lt;Object>(){}</code>. In such a case, the String / byte[] would be given to Jackson
     *     to be interpreted as <code>Object</code> (which means that Jackson will figure out the types on its own), and
     *     that <code>Object</code> gets caster afterwards into your desired type. This case will *NOT* cause a
     *     {@link ClassCastException}. That exception is only thrown the first time someone tries to access some
     *     type-specific content. However, allowing Java to properly figure out the type on its own, can still work, as
     *     the following example shows:
     *
     *     <pre>
     *     List&lt;String> list = getFromJson(col, new TypeRefernce<>(), null);
     *     </pre>
     *     <p>
     *     Whether the column of the given row contains "[1,2,3]" or even "[\"1\", \"2\", \"3\"]", does not really
     *     matter
     *     since Jackson gets in both cases a <code>TypeReference&lt;List&lt;String>></code> as parameter, so Jackson
     *     figures out what it is expected to return, and would convert the integers into a list of strings.
     *     </p>
     *     <p>
     *     However, adding another indirection, i.e. by using a generic helper method that attempts to create a
     *     <code>TypeReference&lt;T></code> would fail.
     *     </p>
     * @param convertFunc
     *     Optional argument to transform the result into a different type. Could be useful to wrap Jackson's internal
     *     List or Map into a specific implementation
     *
     * @return
     *
     * @throws DatabaseException
     */
    public @Nullable <T> T getFromJson(Column col, TypeReference<T> typeRef, @Nullable UnaryOperator<T> convertFunc)
        throws DatabaseException
    {
        @Nullable T ret;
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
                    ret = (OBJ_MAPPER.readValue((String) value, typeRef));
                }
                else if (colSqlType == Types.BLOB)
                {
                    ret = (OBJ_MAPPER.readValue((byte[]) value, typeRef));
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

        if (ret != null && convertFunc != null)
        {
            ret = convertFunc.apply(ret);
        }

        return ret;
    }

    public Short etcdGetShort(Column column)
    {
        return this.<String, @Nullable Short, RuntimeException>build(column, Short::parseShort);
    }

    public Integer etcdGetInt(Column column)
    {
        return this.<String, @Nullable Integer, RuntimeException>build(column, Integer::parseInt);
    }

    public Long etcdGetLong(Column column)
    {
        return this.<String, @Nullable Long, RuntimeException>build(column, Long::parseLong);
    }

    public Boolean etcdGetBoolean(Column column)
    {
        return this.<String, @Nullable Boolean, RuntimeException>build(column, Boolean::parseBoolean);
    }
}
