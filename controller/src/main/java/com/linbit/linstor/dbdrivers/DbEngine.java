package com.linbit.linstor.dbdrivers;

import com.linbit.ExhaustedPoolException;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes.LUKSVolume;
import com.linbit.linstor.core.apicallhandler.controller.db.DbExportPojoData;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.dbdrivers.DatabaseDriverInfo.DatabaseType;
import com.linbit.linstor.dbdrivers.DatabaseTable.Column;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Nodes;
import com.linbit.linstor.dbdrivers.etcd.ETCDEngine;
import com.linbit.linstor.dbdrivers.interfaces.updater.CollectionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.MapDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.dbdrivers.sql.SQLEngine;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.Flags;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.utils.ExceptionThrowingFunction;
import com.linbit.utils.Pair;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * <p>
 * Currently this interface is implemented by {@link SQLEngine} and {@link ETCDEngine}.
 * </p>
 * <p>
 * The idea is that classes like {@link AbsDatabaseDriver} does not have to know if they are
 * working with a database based on SQL or ETCD. This interface contains all necessary methods
 * for interacting with the database like create and delete whole data-sets, or update single columns.
 * </p>
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface DbEngine
{
    /**
     * This interface is only used for log-messages.
     *
     * @param <DATA>
     *     The Linstor-object, i.e. {@link Node}, {@link Resource}, {@link ResourceDefinition}, ...
     */
    @FunctionalInterface
    interface DataToString<DATA>
    {
        String toString(DATA data) throws AccessDeniedException;
    }

    /**
     * The object-specific loader method, creating the actual data object with its initialization-maps
     *
     * @param <DATA>
     *     The Linstor-object, i.e. {@link Node}, {@link Resource}, {@link ResourceDefinition}, ...
     * @param <INIT_MAPS>
     *     The Linstor-object's initialization maps.
     *     For example, if DATA is {@link Node}, then INIT_MAPS should be {@link Node}.{@link Node.InitMaps},
     * @param <LOAD_ALL>
     *     The parent objects needed to create the DATA object. <br/>
     *     For example, {@link Node} does not need anything for initialization, so LOAD_ALL can be
     *     {@link Void}.<br />
     *     {@link Resource} needs a data-structure containing a <code>Map&lt;NodeName, Node&gt;</code> and a
     *     <code>Map&lt;ResourceName, ResourceDefinition&gt;</code>
     */
    @FunctionalInterface
    public interface DataLoader<DATA, INIT_MAPS, LOAD_ALL>
    {
        /**
         * Creates the actual data objects from the given raw data and the already loaded parent-objects.
         * <br/>
         * See {@link DataLoader}'s javadoc for details regarding the generic types
         *
         * @param raw
         * @param parents
         * @throws InvalidNameException
         * @throws InvalidIpAddressException
         * @throws ValueOutOfRangeException
         * @throws DatabaseException
         * @throws MdException
         */
        Pair<DATA, INIT_MAPS> loadImpl(
            RawParameters raw,
            LOAD_ALL parents
        )
            throws InvalidNameException, InvalidIpAddressException, ValueOutOfRangeException, DatabaseException,
            MdException, ValueInUseException, ExhaustedPoolException, AccessDeniedException;
    }

    /**
     * Specifies if the backing driver is SQL or ETCD specific.
     */
    DatabaseType getType();

    /**
     * Returns a {@link StateFlagsPersistence} for the given DATA type.
     *
     * @param <DATA>
     *     The Linstor-object, i.e. {@link Node}, {@link Resource}, {@link ResourceDefinition}, ...
     * @param <FLAG>
     *     The flag-enum for the given DATA type. <br/>
     *     For example {@link Flags}, {@link Flags}, ...
     * @param settersRef
     *     This map contains accessors for all columns of the given {@link Table}.<br/>
     *     The {@link ExceptionThrowingFunction} receives the DATA object, and has to return a
     *     value of correct type for the given {@link Column}. <br />
     *     For example, {@link Nodes#NODE_FLAGS} has to have an {@link ExceptionThrowingFunction}
     *     that gets a {@link Node} which returns a long value from {@link StateFlags#getFlagsBits}
     * @param colRef
     *     The {@link Column} of the FLAG, i.e. {@link Nodes#NODE_FLAGS}
     * @param flagsClassRef
     *     The class of the flag-enum, i.e. <code>Node.NodeFlag.class</code>
     * @param idFormatterRef
     *     Converts the DATA to a String (only for logging)
     */
    <DATA, FLAG extends Enum<FLAG> & Flags> StateFlagsPersistence<DATA> generateFlagsDriver(
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> settersRef,
        Column colRef,
        Class<FLAG> flagsClassRef,
        DataToString<DATA> idFormatterRef
    );

    /**
     * Returns a {@link SingleColumnDatabaseDriver} for the given DATA type where the typeMapper function
     * will also be executed for ETCD types.
     *
     * @param <DATA>
     *     The Linstor-object, i.e. {@link Node}, {@link Resource}, {@link ResourceDefinition}, ...
     * @param <INPUT_TYPE>
     *     The Linstor-type which is updated, i.e. {@link Type}, {@link String} for
     *     {@link ResourceGroup}'s description-driver, <code>byte[]</code>
     *     for the encryption column for {@link LUKSVolume}
     * @param <DB_TYPE>
     *     The type of the database-column. <br />
     *     ETCD will map everything to {@link String} <br />
     *     SQL will need types compatible with the corresponding {@link Column#getSqlType()}
     * @param setters
     *     This map contains accessors for all columns of the given {@link Table}.<br/>
     *     The {@link ExceptionThrowingFunction} receives the DATA object, and has to return a
     *     value of correct type for the given {@link Column}. <br />
     *     For example, {@link Nodes#NODE_FLAGS} has to have an {@link ExceptionThrowingFunction}
     *     that gets a {@link Node} which returns a long value from {@link StateFlags#getFlagsBits}
     * @param colRef
     *     The {@link Column} of the FLAG, i.e. {@link Nodes#NODE_FLAGS}
     * @param typeMapperRef
     *     Converts the INPUT_TYPE to DB_TYPE
     * @param dataToStringRef
     *     Converts the DATA to a String (only for logging)
     * @param dataValueToStringRef
     *     Converts the INPUT_TYPE to {@link String} (only for logging)
     */
    <DATA, INPUT_TYPE, DB_TYPE> SingleColumnDatabaseDriver<DATA, INPUT_TYPE> generateSingleColumnDriver(
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> setters,
        Column colRef,
        Function<INPUT_TYPE, DB_TYPE> typeMapperRef,
        DataToString<DATA> dataToStringRef,
        ExceptionThrowingFunction<DATA, String, AccessDeniedException> dataValueToStringRef,
        DataToString<INPUT_TYPE> inputToStringRef
    );

    /**
     * Returns a {@link CollectionDatabaseDriver} for the given DATA type.
     *
     * @param <DATA>
     *     The Linstor-object, i.e. {@link Node}, {@link Resource}, {@link ResourceDefinition}, ...
     * @param <LIST_TYPE>
     *     The type of the elements of the {@link Collection}.
     * @param setters
     *     This map contains accessors for all columns of the given {@link Table}.<br/>
     *     The {@link ExceptionThrowingFunction} receives the DATA object, and has to return a
     *     value of correct type for the given {@link Column}. <br />
     *     For example, {@link Nodes#NODE_FLAGS} has to have an {@link ExceptionThrowingFunction}
     *     that gets a {@link Node} which returns a long value from {@link StateFlags#getFlagsBits}
     * @param colRef
     *     The {@link Column} of the FLAG, i.e. {@link Nodes#NODE_FLAGS}
     * @param dataToStringRef
     *     Converts the DATA to a String (only for logging)
     */
    <DATA, LIST_TYPE> CollectionDatabaseDriver<DATA, LIST_TYPE> generateCollectionToJsonStringArrayDriver(
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> setters,
        Column colRef,
        DataToString<DATA> dataToStringRef
    );

    /**
     * Returns a {@link MapDatabaseDriver} for the given DATA type.
     *
     * @param <DATA>
     *     The Linstor-object, i.e. {@link Node}, {@link Resource}, {@link ResourceDefinition}, ...
     * @param <LIST_TYPE>
     *     The type of the elements of the {@link Collection}.
     * @param setters
     *     This map contains accessors for all columns of the given {@link Table}.<br/>
     *     The {@link ExceptionThrowingFunction} receives the DATA object, and has to return a
     *     value of correct type for the given {@link Column}. <br />
     *     For example, {@link Nodes#NODE_FLAGS} has to have an {@link ExceptionThrowingFunction}
     *     that gets a {@link Node} which returns a long value from {@link StateFlags#getFlagsBits}
     * @param colRef
     *     The {@link Column} of the FLAG, i.e. {@link Nodes#NODE_FLAGS}
     * @param dataToStringRef
     *     Converts the DATA to a String (only for logging)
     */
    <DATA, KEY, VALUE> MapDatabaseDriver<DATA, KEY, VALUE> generateMapToJsonStringArrayDriver(
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> setters,
        Column colRef,
        DataToString<DATA> dataToStringRef
    );

    /**
     * Persists the given data object into the database.
     *
     * @param <DATA>
     *     The Linstor-object, i.e. {@link Node}, {@link Resource}, {@link ResourceDefinition}, ...
     * @param setters
     *     This map contains accessors for all columns of the given {@link Table}.<br/>
     *     The {@link ExceptionThrowingFunction} receives the DATA object, and has to return a
     *     value of correct type for the given {@link Column}. <br />
     *     For example, {@link Nodes#NODE_FLAGS} has to have an {@link ExceptionThrowingFunction}
     *     that gets a {@link Node} which returns a long value from {@link StateFlags#getFlagsBits}
     * @param dataRef
     *     The actual data which should be persisted
     * @param table
     *     The {@link Table} in which the given <code>dataRef</code> should be persisted.
     * @param dataToString
     *     Converts the DATA to a String (only for logging)
     *
     * @throws DatabaseException
     * @throws AccessDeniedException
     */
    <DATA> void create(
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> setters,
        DATA dataRef,
        DatabaseTable table,
        DataToString<DATA> dataToString
    )
        throws DatabaseException, AccessDeniedException;

    /**
     * Update or inserts (creates) the given DATA object
     *
     * @param <DATA>
     *     The Linstor-object, i.e. {@link Node}, {@link Resource}, {@link ResourceDefinition}, ...
     * @param settersRef
     *     This map contains accessors for all columns of the given {@link Table}.<br/>
     *     The {@link ExceptionThrowingFunction} receives the DATA object, and has to return a
     *     value of correct type for the given {@link Column}. <br />
     *     For example, {@link Nodes#NODE_FLAGS} has to have an {@link ExceptionThrowingFunction}
     *     that gets a {@link Node} which returns a long value from {@link StateFlags#getFlagsBits}
     * @param dataRef
     *     The actual data which should be upserted
     * @param table
     *     The {@link Table} in which the given <code>dataRef</code> should be upserted.
     * @param dataToString
     *     Converts the DATA to a String (only for logging)
     *
     * @throws DatabaseException
     * @throws AccessDeniedException
     */
    <DATA> void upsert(
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> settersRef,
        DATA dataRef,
        DatabaseTable tableRef,
        DataToString<DATA> dataToString
    )
        throws DatabaseException, AccessDeniedException;

    /**
     * Deletes the given data object from the database.
     *
     * @param <DATA>
     *     The Linstor-object, i.e. {@link Node}, {@link Resource}, {@link ResourceDefinition}, ...
     * @param setters
     *     This map contains accessors for all columns of the given {@link Table}.<br/>
     *     Required for accessing the primary key<br />
     *     The {@link ExceptionThrowingFunction} receives the DATA object, and has to return a
     *     value of correct type for the given {@link Column}. <br />
     *     For example, {@link Nodes#NODE_FLAGS} has to have an {@link ExceptionThrowingFunction}
     *     that gets a {@link Node} which returns a long value from {@link StateFlags#getFlagsBits}
     * @param dataRef
     *     The actual data which should be deleted
     * @param table
     *     The {@link Table} from which the given <code>dataRef</code> should be deleted.
     * @param dataToString
     *     Converts the DATA to a String (only for logging)
     *
     * @throws DatabaseException
     * @throws AccessDeniedException
     */
    <DATA> void delete(
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> setters,
        DATA dataRef,
        DatabaseTable table,
        DataToString<DATA> dataToString
    )
        throws DatabaseException, AccessDeniedException;

    /**
     * Deletes all data objects from the given database table.
     *
     * @param table
     *     The {@link Table} from which should be truncated / cleared.
     *
     * @throws DatabaseException
     */
    void truncate(DatabaseTable table) throws DatabaseException;

    /**
     * Loads all objects from the given database. It is required that all parent-objects
     * are already loaded (i.e. loading all {@link Resource}s requires that all {@link Node}s and
     * {@link ResourceDefinition}s are fully loaded)
     *
     * @param <DATA>
     *     The Linstor-object, i.e. {@link Node}, {@link Resource}, {@link ResourceDefinition}, ...
     * @param <INIT_MAPS>
     *     The Linstor-object's initialization maps.
     *     For example, if DATA is {@link Node}, then INIT_MAPS should be {@link Node.InitMaps},
     * @param <LOAD_ALL>
     *     The parent objects needed to create the DATA object. <br/>
     *     For example, {@link Node} does not need anything for initialization, so LOAD_ALL can be
     *     {@link Void}.<br />
     *     {@link Resource} needs a data-structure containing a <code>Map&lt;NodeName, Node&gt;</code> and a
     *     <code>Map&lt;ResourceName, ResourceDefinition&gt;</code>
     * @param table
     *     The {@link Table} from which the given DATA objects should be loaded
     * @param parents
     *     The data-structure containing all loaded parent objects
     * @param dataLoaderRef
     *     The implementation of restoring the DATA object from the raw input and the parent objects
     * @throws DatabaseException
     * @throws AccessDeniedException
     * @throws InvalidNameException
     * @throws InvalidIpAddressException
     * @throws ValueOutOfRangeException
     * @throws MdException
     */
    <DATA extends Comparable<? super DATA>, INIT_MAPS, LOAD_ALL> Map<DATA, INIT_MAPS> loadAll(
        DatabaseTable table,
        LOAD_ALL parents,
        DataLoader<DATA, INIT_MAPS, LOAD_ALL> dataLoaderRef
    )
        throws DatabaseException, AccessDeniedException, InvalidNameException, InvalidIpAddressException,
        ValueOutOfRangeException, MdException, ValueInUseException, ExhaustedPoolException, AccessDeniedException;

    String getDbDump() throws DatabaseException;

    ApiCallRc backupDb(String backupPath) throws DatabaseException;

    List<RawParameters> export(DatabaseTable tableRef) throws DatabaseException;

    /**
     * Unlike the {@link #truncate(DatabaseTable)} method, this method is not linked to the current
     * {@link DatabaseTable} instances and can therefore also truncate tables that existed in previous versions but no
     * longer exist in the current {@link GeneratedDatabaseTables} class.
     *
     * @param orderedTablesListRef
     *
     * @throws DatabaseException
     */
    void truncateAllData(List<DbExportPojoData.Table> orderedTablesListRef) throws DatabaseException;

    void importData(DbExportPojoData.Table dbExportTableRef) throws DatabaseException;
}
