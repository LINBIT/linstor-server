package com.linbit.linstor.propscon;

import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.TransactionMgr;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorSqlRuntimeException;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.dbdrivers.interfaces.PropsConDatabaseDriver;
import com.linbit.utils.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Hierarchical properties container
 *
 * ** IMPORTANT SYNCHRONIZATION NOTICE **
 * External synchronization is required if multiple threads are using the container concurrently.
 * Concurrent threads must synchronize access to the entire hierarchy of properties containers,
 * not just access to a subcontainer, because many subcontainer actions will also affect the
 * root container of the hierarchy.
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class PropsContainer implements Props
{
    private static final String PATH_STOR_POOL                = "/storPoolConf/";
    private static final String PATH_STOR_POOL_DFN            = "/storPoolDfnConf/";
    private static final String PATH_NODES                    = "/nodes/";
    private static final String PATH_RESOURCE_DEFINITIONS     = "/resourcedefinitions/";
    private static final String PATH_RESOURCES                = "/resources/";
    private static final String PATH_VOLUME_DEFINITIONS       = "/volumedefinitions/";
    private static final String PATH_VOLUMES                  = "/volumes/";
    private static final String PATH_NODE_CON_DEFINITIONS     = "/conDfn/nodes/";
    private static final String PATH_RESOURCE_CON_DEFINITIONS = "/conDfn/resources/";
    private static final String PATH_VOLUME_CON_DEFINITIONS   = "/conDfn/volume/";

    public static final int PATH_MAX_LENGTH = 256;

    private PropsContainer rootContainer;
    private PropsContainer parentContainer;
    private String containerKey;
    private int itemCount;
    private Map<String, String> propMap;
    private Map<String, PropsContainer> containerMap;

    private static final int PATH_NAMESPACE = 0;
    private static final int PATH_KEY = 1;

    private Map<String, String> mapAccessor;
    private Set<String> keySetAccessor;
    private Set<Map.Entry<String, String>> entrySetAccessor;
    private Collection<String> valuesCollectionAccessor;

    protected final PropsConDatabaseDriver dbDriver;
    protected TransactionMgr transMgr;
    private Map<String, String> cachedPropMap;

    private boolean initialized = false;
    protected String instanceName;

    PropsContainer(
        String key,
        PropsContainer parent,
        PropsConDatabaseDriver dbDriverRef
    ) throws InvalidKeyException
    {
        dbDriver = dbDriverRef;

        if (key == null && parent == null)
        {
            // Create root PropsContainer

            containerKey = PATH_SEPARATOR;

            rootContainer = this;
            parentContainer = null;
            cachedPropMap = new HashMap<>();
        }
        else
        {
            // Create sub PropsContainer

            ErrorCheck.ctorNotNull(PropsContainer.class, String.class, key);

            checkKey(key);
            containerKey = key;

            rootContainer = parent.getRoot();
            parentContainer = parent;
            cachedPropMap = null;
        }
        propMap = new TreeMap<>();
        containerMap = new TreeMap<>();

        keySetAccessor = null;
        entrySetAccessor = null;
        mapAccessor = null;
        valuesCollectionAccessor = null;
    }

    @Override
    public synchronized Map<String, String> map()
    {
        if (mapAccessor == null)
        {
            mapAccessor = new PropsContainer.PropsConMap(this);
        }
        return mapAccessor;
    }

    @Override
    public synchronized Set<String> keySet()
    {
        if (keySetAccessor == null)
        {
            keySetAccessor = new PropsContainer.KeySet(this);
        }
        return keySetAccessor;
    }

    @Override
    public synchronized Set<Map.Entry<String, String>> entrySet()
    {
        if (entrySetAccessor == null)
        {
            entrySetAccessor = new PropsContainer.EntrySet(this);
        }
        return entrySetAccessor;
    }

    @Override
    public synchronized Collection<String> values()
    {
        if (valuesCollectionAccessor == null)
        {
            valuesCollectionAccessor = new PropsContainer.ValuesCollection(this);
        }
        return valuesCollectionAccessor;
    }

    /**
     * Returns the number of elements (properties and subcontainers)
     * contained in this container hierarchy
     *
     * @return The number of elements in the container hierarchy
     */
    @Override
    public int size()
    {
        return itemCount;
    }

    /**
     * Indicates whether there are any elements in the container hierarchy
     *
     * @return true if there are any properties contained in the container
     *     hierarchy, otherwise false
     */
    @Override
    public boolean isEmpty()
    {
        return itemCount == 0;
    }

    /**
     * Returns the property if found.
     *
     * getProp("a", "b/c") has the same effect as getProp("b/c/a", null)
     *
     * Also, if the dbCon class variable is set (via {@link PropsContainer#setConnection(Connection)})
     * this operation is also persisted to the database
     *
     * @param key
     * @param namespace
     * @return
     * @throws InvalidKeyException
     */
    @Override
    public String getProp(String key, String namespace) throws InvalidKeyException
    {
        String[] pathElements = splitPath(namespace, key);
        checkKey(pathElements[PATH_KEY]);

        PropsContainer con = findNamespace(pathElements[PATH_NAMESPACE]);
        String value = null;
        if (con != null)
        {
            value = con.propMap.get(pathElements[PATH_KEY]);
        }
        return value;
    }

    /**
     * Sets the given property.
     * Creates all necessary non-existent namespaces.
     *
     * setProp("a", "value", "b/c") has the same effect as setProp("b/c/a", "value", null)
     *
     * Also, if the dbCon class variable is set (via {@link PropsContainer#setConnection(Connection)})
     * this operation is also persisted to the database
     *
     * @param key
     * @param value
     * @param namespace
     * @return
     * @throws InvalidKeyException
     * @throws InvalidValueException
     * @throws SQLException
     */
    @Override
    public String setProp(String key, String value, String namespace)
        throws InvalidKeyException, InvalidValueException, SQLException
    {
        if (value == null)
        {
            throw new InvalidValueException();
        }

        String[] pathElements = splitPath(namespace, key);
        String actualKey = pathElements[PATH_KEY];
        checkKey(actualKey);

        PropsContainer con = ensureNamespaceExists(pathElements[PATH_NAMESPACE]);
        String oldValue = con.propMap.put(actualKey, value);
        if (oldValue == null)
        {
            con.modifySize(1);
        }
        dbPersist(con.getPath() + actualKey, value, oldValue);
        return oldValue;
    }

    /**
     * Removes the specified property and returns the old value (could be null).
     *
     * removeProp("a", "b/c") has the same effect as removeProp("b/c/a", null)
     *
     * Also, if the dbCon class variable is set (via {@link PropsContainer#setConnection(Connection)})
     * this operation is also persisted to the database
     *
     * @param key
     * @param namespace
     * @return
     * @throws InvalidKeyException
     * @throws SQLException
     */
    @Override
    public String removeProp(String key, String namespace)
        throws InvalidKeyException, SQLException
    {
        String value = null;
        String[] pathElements = splitPath(namespace, key);
        String actualKey = pathElements[PATH_KEY];
        checkKey(actualKey);

        PropsContainer con = findNamespace(pathElements[PATH_NAMESPACE]);
        if (con != null)
        {
            value = con.propMap.remove(actualKey);
            dbRemove(con.getPath() + actualKey, value);
            if (value != null)
            {
                con.modifySize(-1);
                con.removeCleanup();
            }
        }
        return value;
    }

    /**
     * Sets all props from the given map into the given namespace.
     * The namespace parameter acts as a prefix for all keys of the map.
     *
     * Returns true if any property has been modified by this method.
     *
     * Also, if the dbCon class variable is set (via {@link PropsContainer#setConnection(Connection)})
     * this operation is also persisted to the database
     *
     * @param entryMap
     * @param namespace
     * @throws Exception
     */
    public boolean setAllProps(Map<? extends String, ? extends String> entryMap, String namespace)
        throws InvalidKeyException, InvalidValueException, SQLException
    {
        boolean modified = false;
        for (Map.Entry<? extends String, ? extends String> entry : entryMap.entrySet())
        {
            String key = entry.getKey();
            String value = entry.getValue();

            if (value == null)
            {
                rollback();
                throw new InvalidValueException("Value must not be null");
            }

            String[] pathElements = splitPath(namespace, key);
            String actualKey = pathElements[PATH_KEY];
            checkKey(actualKey);

            PropsContainer con = ensureNamespaceExists(pathElements[PATH_NAMESPACE]);
            String oldValue = con.propMap.put(actualKey, value);
            dbPersist(key, value, oldValue);
            if (oldValue == null)
            {
                con.modifySize(1);
                modified = true;
            }
        }
        return modified;
    }

    /**
     * Removes all properties from the given set.
     * The namespace parameter acts as a prefix for all given properties.
     *
     * Returns true if any property has been modified by this method
     *
     * Also, if the dbCon class variable is set (via {@link PropsContainer#setConnection(Connection)})
     * this operation is also persisted to the database
     *
     * @param selection
     * @param namespace
     * @throws SQLException
     */
    boolean removeAllProps(Set<String> selection, String namespace) throws SQLException
    {
        boolean changed = false;
        for (String key : selection)
        {
            try
            {
                String[] pathElements = splitPath(namespace, key);
                String actualKey = pathElements[PATH_KEY];
                checkKey(actualKey);

                PropsContainer con = findNamespace(pathElements[PATH_NAMESPACE]);
                if (con != null)
                {
                    String value = con.propMap.remove(actualKey);

                    if (value != null)
                    {
                        con.modifySize(-1);
                        con.removeCleanup();
                        changed = true;

                        dbRemove(con.getPath() + actualKey, value);
                    }
                }
            }
            catch (InvalidKeyException ignored)
            {
            }
        }
        return changed;
    }

    /**
     * Retains all properties of the given set.
     * The namespace acts as a prefix for the properties of the set.
     *
     * Returns true if any property has been modified by this method
     *
     * Also, if the dbCon class variable is set (via {@link PropsContainer#setConnection(Connection)})
     * this operation is also persisted to the database
     * @param selection
     * @param namespace
     * @throws SQLException
     */
    boolean retainAllProps(Set<String> selection, String namespace) throws SQLException
    {
        boolean changed = false;
        Set<String> removeSet = new TreeSet<>();
        Iterator<String> keysIter = keysIterator();
        while (keysIter.hasNext())
        {
            String key = keysIter.next();
            if (!selection.contains(key))
            {
                removeSet.add(key);
            }
        }
        changed = removeAllProps(removeSet, namespace);
        return changed;
    }

    @Override
    public void delete() throws SQLException
    {
        clear();
    }

    /**
     * Removes all properties from this instance.
     *
     * Also, if the dbCon class variable is set (via {@link PropsContainer#setConnection(Connection)})
     * this operation is also persisted to the database
     *
     * @throws SQLException
     */
    @Override
    public void clear() throws SQLException
    {
        containerMap.clear();
        propMap.clear();
        dbRemoveAll();
        if (parentContainer != null)
        {
            parentContainer.modifySize(itemCount * -1);
        }
        removeCleanup();
        itemCount = 0;
    }

    /**
     * Returns the property if found.
     *
     * getProp("a") has the same effect as getProp("a", null)
     *
     * Also, if the dbCon class variable is set (via {@link PropsContainer#setConnection(Connection)})
     * this operation is also persisted to the database
     *
     * @param key
     * @return
     * @throws InvalidKeyException
     */
    @Override
    public String getProp(String key) throws InvalidKeyException
    {
        return getProp(key, null);
    }

    /**
     * Sets the given property.
     * Creates all necessary non-existent namespaces.
     *
     * setProp("a", "value") has the same effect as setProp("a", "value", null)
     *
     * Also, if the dbCon class variable is set (via {@link PropsContainer#setConnection(Connection)})
     * this operation is also persisted to the database
     *
     * @param key
     * @param value
     * @return
     * @throws InvalidKeyException
     * @throws InvalidValueException
     * @throws SQLException
     */
    @Override
    public String setProp(String key, String value)
        throws InvalidKeyException, InvalidValueException, SQLException
    {
        return setProp(key, value, null);
    }

    /**
     * Removes the specified property and returns the old value (could be null).
     *
     * removeProp("a") has the same effect as removeProp("a", null)
     *
     * Also, if the dbCon class variable is set (via {@link PropsContainer#setConnection(Connection)})
     * this operation is also persisted to the database
     *
     * @param key
     * @return
     * @throws InvalidKeyException
     * @throws SQLException
     */
    @Override
    public String removeProp(String key)
        throws InvalidKeyException, SQLException
    {
        return removeProp(key, null);
    }

    /**
     * TODO: Experimental: non-recursive iterator over all namespace properties
     *
     * @param namespace
     * @return
     */
    Iterator<Map.Entry<String, String>> iterateProps()
    {
        // unmodifiable so that we do not have to bother with .remove() and db-persistence
        return Collections.unmodifiableMap(propMap).entrySet().iterator();
    }

    /**
     * TODO: Experimental: non-recursive iterator over all namespace containers
     *
     * @param namespace
     * @return
     */
    Iterator<PropsContainer> iterateContainers()
    {
        // unmodifiable so that we do not have to bother with .remove() and db-persistence
        return Collections.unmodifiableMap(containerMap).values().iterator();
    }

    /**
     * Modifies the size (number of elements) of all containers on the path from the
     * current container to the root container.
     *
     * @param diff The amount by which to change the number of elements in the container
     */
    void modifySize(int diff)
    {
        if (diff < 0)
        {
            if (itemCount + diff < 0)
            {
                throw new ImplementationError(
                    "Container count indicates less than zero elements",
                    new ValueOutOfRangeException(ValueOutOfRangeException.ViolationType.TOO_LOW)
                );
            }
        }
        else
            if (diff > 0)
            {
                if (Integer.MAX_VALUE - itemCount < diff)
                {
                    throw new ImplementationError(
                        "Attempt to increase the container's count to more than Integer.MAX_VALUE elements",
                        new ValueOutOfRangeException(ValueOutOfRangeException.ViolationType.TOO_HIGH)
                    );
                }
            }
        itemCount += diff;
        if (parentContainer != null)
        {
            parentContainer.modifySize(diff);
        }
    }

    private String sanitizePath(String path, boolean forceRelative) throws InvalidKeyException
    {
        int pathLength = path.length();
        if (pathLength > PATH_MAX_LENGTH)
        {
            throw new InvalidKeyException(path);
        }

        StringBuilder sanitizedPath = new StringBuilder();
        StringTokenizer tokens = new StringTokenizer(path, PATH_SEPARATOR);

        if (path.startsWith(PATH_SEPARATOR) && !forceRelative)
        {
            sanitizedPath.append(PATH_SEPARATOR);
        }
        boolean empty = true;
        while (tokens.hasMoreTokens())
        {
            String item = tokens.nextToken();
            if (item.length() > 0)
            {
                if (!empty)
                {
                    sanitizedPath.append(PATH_SEPARATOR);
                }
                sanitizedPath.append(item);
                empty = false;
            }
        }

        return sanitizedPath.toString();
    }

    private String[] splitPath(String namespace, String path) throws InvalidKeyException
    {
        if (path == null)
        {
            throw new InvalidKeyException(path);
        }

        String safePath;
        if (namespace != null)
        {
            String safeNamespace = sanitizePath(namespace, false);

            // If a namespace was specified, the path is always relative to the namespace
            safePath = sanitizePath(path, true);

            if (safeNamespace.length() == 0 || safeNamespace.endsWith(PATH_SEPARATOR))
            {
                safePath = safeNamespace + safePath;
            }
            else
            {
                safePath = safeNamespace + PATH_SEPARATOR + safePath;
            }
        }
        else
        {
            // No namespace specified, path can be relative or absolute
            safePath = sanitizePath(path, false);
        }

        String[] pathElements = new String[2];
        int index = safePath.lastIndexOf('/');
        int pathLength;
        if (index != -1)
        {
            pathElements[0] = safePath.substring(0, index + 1);
            pathElements[1] = safePath.substring(index + 1, safePath.length());
            pathLength = pathElements[0].length() + pathElements[1].length() + 1;
        }
        else
        {
            pathElements[0] = null;
            pathElements[1] = safePath;
            pathLength = pathElements[1].length() + 1;
        }
        if (pathLength > PATH_MAX_LENGTH)
        {
            throw new InvalidKeyException(path);
        }
        return pathElements;
    }

    /**
     * Returns the PropsContainer that constitutes the specified namespace, or null if the namespace
     * does not exist
     *
     * @param namespace The name of the namespace that should be returned
     * @return The namespace's PropsContainer, or null, if the namespace does not exist
     * @throws InvalidKeyException If the namespace specification is invalid
     */
    @Override
    public Props getNamespace(String namespace) throws InvalidKeyException
    {
        return findNamespace(namespace);
    }

    /**
     * Iterates over the first level namespaces of the current PropsContainer
     *
     * @return An Iterator containing the keys of the namepsaces
     */
    @Override
    public Iterator<String> iterateNamespaces()
    {
        return Collections.unmodifiableMap(containerMap).keySet().iterator();
    }

    /**
     * Returns the PropsContainer that constitutes the specified namespace, or null if the namespace
     * does not exist
     *
     * @param namespace The name of the namespace that should be returned
     * @return The namespace's PropsContainer, or null, if the namespace does not exist
     * @throws InvalidKeyException If the namespace specification is invalid
     */
    private PropsContainer findNamespace(String namespace)
    {
        PropsContainer con = this;
        if (namespace != null)
        {
            if (namespace.startsWith(PATH_SEPARATOR))
            {
                con = getRoot();
            }
            StringTokenizer tokens = new StringTokenizer(namespace, PATH_SEPARATOR);
            while (tokens.hasMoreTokens() && con != null)
            {
                con = con.containerMap.get(tokens.nextToken());
            }
        }
        return con;
    }

    /**
     * Returns the map used to save the properties in the current namespace
     *
     * Currently this method is used by the SerialPropsCon to bypass set*Prop methods
     * @return
     */
    protected Map<String, String> getRawPropMap()
    {
        return propMap;
    }

    /**
     * Creates the path to the specified namespace if it does not exist already
     *
     * @param namespace Path to the namespace
     * @return PropsContainer that constitutes the specified namespace
     * @throws InvalidKeyException If the namespace path is invalid
     * @throws SQLException
     */
    protected PropsContainer ensureNamespaceExists(String namespace) throws InvalidKeyException, SQLException
    {
        PropsContainer con = this;
        try
        {
            if (namespace != null)
            {
                if (namespace.startsWith(PATH_SEPARATOR))
                {
                    con = getRoot();
                }
                StringTokenizer tokens = new StringTokenizer(namespace, PATH_SEPARATOR);
                while (tokens.hasMoreTokens())
                {
                    String key = tokens.nextToken();
                    PropsContainer subCon = con.containerMap.get(key);
                    if (subCon == null)
                    {
                        subCon = createSubContainer(key, con);
                        con.containerMap.put(key, subCon);
                    }
                    con = subCon;
                }
            }
        }
        catch (InvalidKeyException keyExc)
        {
            con.removeCleanup();
            throw keyExc;
        }
        return con;
    }

    @Override
    public String getPath()
    {
        StringBuilder pathComponents = getPathComponents();
        return pathComponents.toString();
    }

    private StringBuilder getPathComponents()
    {
        StringBuilder pathComponents;
        if (parentContainer == null)
        {
            pathComponents = new StringBuilder();
        }
        else
        {
            pathComponents = parentContainer.getPathComponents();
            pathComponents.append(containerKey);
            pathComponents.append(PATH_SEPARATOR);
        }
        return pathComponents;
    }

    @SuppressWarnings("unused") // for the throw of SQLException - which is needed by SerialPropsContainer
    PropsContainer createSubContainer(String key, PropsContainer con) throws InvalidKeyException, SQLException
    {
        return new PropsContainer(key, con, dbDriver);
    }

    private PropsContainer getRoot()
    {
        return rootContainer;
    }

    private void removeCleanup()
    {
        if (propMap.isEmpty() && containerMap.isEmpty())
        {
            if (parentContainer != null)
            {
                parentContainer.containerMap.remove(containerKey);
                parentContainer.removeCleanup();
            }
        }
    }

    private static void checkKey(String key) throws InvalidKeyException
    {
        if (key.contains(PATH_SEPARATOR))
        {
            throw new InvalidKeyException(key);
        }
    }

    private void collectAllKeys(String prefix, Set<String> collector, boolean recursive)
    {
        for (String key : propMap.keySet())
        {
            collector.add(prefix + key);
        }
        if (recursive)
        {
            for (PropsContainer subCon : containerMap.values())
            {
                subCon.collectAllKeys(prefix + subCon.containerKey + PATH_SEPARATOR, collector, recursive);
            }
        }
    }

    private void collectAllEntries(String prefix, Map<String, String> collector, boolean recursive)
    {
        for (Map.Entry<String, String> item : propMap.entrySet())
        {
            collector.put(prefix + item.getKey(), item.getValue());
        }
        if (recursive)
        {
            for (PropsContainer subCon : containerMap.values())
            {
                subCon.collectAllEntries(prefix + subCon.containerKey + PATH_SEPARATOR, collector, recursive);
            }
        }
    }

    private void collectAllValues(Collection<String> collector, boolean recursive)
    {
        collector.addAll(propMap.values());
        if (recursive)
        {
            for (PropsContainer subCon : containerMap.values())
            {
                subCon.collectAllValues(collector, recursive);
            }
        }
    }

    @Override
    public Iterator<Map.Entry<String, String>> iterator()
    {
        return new EntriesIterator(this);
    }

    @Override
    public Iterator<String> keysIterator()
    {
        return new KeysIterator(this);
    }

    @Override
    public Iterator<String> valuesIterator()
    {
        return new ValuesIterator(this);
    }

    @Override
    public void initialized()
    {
        initialized = true;
    }

    @Override
    public boolean isInitialized()
    {
        return initialized;
    }

    @Override
    public void setConnection(TransactionMgr transMgrRef) throws ImplementationError
    {
        if (!hasTransMgr() && isDirtyWithoutTransMgr())
        {
            throw new ImplementationError("setConnection was called AFTER data was manipulated", null);
        }
        if (transMgr != null && transMgrRef != null && transMgrRef != transMgr)
        {
            throw new ImplementationError("attempt to replace an active transMgr", null);
        }
        if (transMgrRef != null)
        {
            transMgrRef.register(rootContainer);
        }
        transMgr = transMgrRef;
    }

    @Override
    public boolean isDirty()
    {
        return !rootContainer.cachedPropMap.isEmpty();
    }

    @Override
    public boolean isDirtyWithoutTransMgr()
    {
        return !hasTransMgr() && isDirty();
    }

    private void cache(String key, String value)
    {
        if (initialized)
        {
            if (!rootContainer.cachedPropMap.containsKey(key))
            {
                rootContainer.cachedPropMap.put(key, value);
            }
        }
    }

    @Override
    public boolean hasTransMgr()
    {
        return transMgr != null;
    }

    @Override
    public void commit()
    {
        rootContainer.cachedPropMap.clear();
    }

    @Override
    public void rollback()
    {
        PropsContainer root = rootContainer;
        for (Entry<String, String> entry : root.cachedPropMap.entrySet())
        {
            String key = entry.getKey();
            String value = entry.getValue();

            try
            {
                // do not use root.setProp or root.removeProp
                // as those will trigger again a database update and a caching update.
                // Thus, we just use raw propMap to bypass the database and cachings
                PropsContainer targetContainer = root;
                int idx = key.lastIndexOf(PATH_SEPARATOR);
                if (idx != -1)
                {
                    targetContainer = root.ensureNamespaceExists(key.substring(0, idx));
                }
                String oldValue;
                if (value == null)
                {
                    oldValue = targetContainer.propMap.remove(key);
                }
                else
                {
                    oldValue = targetContainer.propMap.put(key, value);
                }
                if (oldValue == null)
                {
                    targetContainer.modifySize(1);
                }
            }
            catch (InvalidKeyException | SQLException exc)
            {
                // cannot happen
                throw new ImplementationError(
                    "Rolling back propsContainer threw an exception.",
                    exc
                );
            }
        }
        root.cachedPropMap.clear();
    }

    private void dbPersist(String key, String value, String oldValue) throws SQLException
    {
        if (initialized)
        {
            cache(key, oldValue);
            if (dbDriver != null)
            {
                try
                {
                    dbDriver.persist(instanceName, key, value, transMgr);
                }
                catch (SQLException sqlExc)
                {
                    rollback();
                    throw sqlExc;
                }
            }
        }
    }

    private void dbRemove(String key, String oldValue) throws SQLException
    {
        if (initialized)
        {
            cache(key, oldValue);
            if (dbDriver != null)
            {
                try
                {
                    dbDriver.remove(instanceName, key, transMgr);
                }
                catch (SQLException sqlExc)
                {
                    rollback();
                    throw sqlExc;
                }
            }
        }
    }

    private void dbRemoveAll() throws SQLException
    {
        if (initialized)
        {
            Set<Entry<String, String>> entrySet = rootContainer.entrySet();
            for (Entry<String, String> entry : entrySet)
            {
                cache(entry.getKey(), entry.getValue());
            }

            if (dbDriver != null)
            {
                try
                {
                    dbDriver.removeAll(instanceName, transMgr);
                }
                catch (SQLException sqlExc)
                {
                    rollback();
                    throw sqlExc;
                }
            }
        }
    }

    class PropsConMap implements Map<String, String>
    {
        private PropsContainer container;

        PropsConMap(PropsContainer con)
        {
            container = con;
        }

        @Override
        public int size()
        {
            return container.itemCount;
        }

        @Override
        public boolean isEmpty()
        {
            return container.itemCount == 0;
        }

        @Override
        public boolean containsKey(Object key)
        {
            boolean result = false;
            try
            {
                String[] pathElements = container.splitPath(null, (String) key);

                PropsContainer con = container.findNamespace(pathElements[PATH_NAMESPACE]);
                if (con != null)
                {
                    result = con.propMap.containsKey(pathElements[PATH_KEY]);
                }
            }
            catch (ClassCastException castExc)
            {
                throw new ImplementationError(
                    "Key for map operation is of illegal object type",
                    castExc
                );
            }
            catch (InvalidKeyException keyExc)
            {
                throw new IllegalArgumentException(
                    "Key for map operation violates validity constraints",
                    keyExc
                );
            }
            return result;
        }

        @Override
        public boolean containsValue(Object value)
        {
            boolean result = false;
            try
            {
                if (container.propMap.containsValue(value))
                {
                    result = true;
                }
                else
                {
                    for (PropsContainer subCon : container.containerMap.values())
                    {
                        if (subCon.map().containsValue(value))
                        {
                            result = true;
                            break;
                        }
                    }
                }
            }
            catch (ClassCastException castExc)
            {
                throw new ImplementationError(
                    "Key for map operation is of illegal object type",
                    castExc
                );
            }
            return result;
        }

        @Override
        public String get(Object key)
        {
            String value = null;
            try
            {
                value = container.getProp((String) key, null);
            }
            catch (ClassCastException castExc)
            {
                throw new ImplementationError(
                    "Key for map operation is of illegal object type",
                    castExc
                );
            }
            catch (InvalidKeyException keyExc)
            {
                throw new IllegalArgumentException(
                    "Key for map operation violates validity constraints",
                    keyExc
                );
            }
            return value;
        }

        @Override
        public String put(String key, String value)
        {
            String oldValue = null;
            try
            {
                oldValue = container.setProp(key, value, null);
            }
            catch (InvalidKeyException | InvalidValueException invData)
            {
                throw new IllegalArgumentException(
                    "Map values to insert violate validity constraints",
                    invData
                );
            }
            catch (SQLException sqlExc)
            {
                rollback();
                throw new LinStorSqlRuntimeException(
                    "Failed to add or update entries in the properties container " + instanceName,
                    sqlExc
                );
            }

            return oldValue;
        }

        @Override
        public String remove(final Object key)
        {
            String value = null;
            boolean unknownType = false;
            Exception unknownTypeCause = null;
            try
            {
                Object effKey = key;
                if (effKey instanceof Map.Entry)
                {
                    Map.Entry<?, ?> entry = (Map.Entry<?, ?>) effKey;
                    effKey = entry.getKey();
                }

                if (effKey instanceof String)
                {
                    value = container.removeProp((String) effKey, null);
                }
                else
                {
                    unknownType = true;
                }
            }
            catch (ClassCastException castExc)
            {
                unknownType = true;
                unknownTypeCause = castExc;
            }
            catch (InvalidKeyException keyExc)
            {
                throw new IllegalArgumentException(
                    "Key for map operation violates validity constraints",
                    keyExc
                );
            }
            catch (SQLException sqlExc)
            {
                throw new LinStorSqlRuntimeException(
                    "Failed to remove entries from the properties container " + instanceName,
                    sqlExc
                );
            }

            if (unknownType)
            {
                throw new ImplementationError(
                    "Key for map operation is of illegal object type",
                    unknownTypeCause
                );
            }
            return value;
        }

        @Override
        public void putAll(Map<? extends String, ? extends String> entryMap)
        {
            try
            {
                container.setAllProps(entryMap, null);
            }
            catch (InvalidKeyException keyExc)
            {
                throw new IllegalArgumentException(
                    "Key for map operation violates validity constraints",
                    keyExc
                );
            }
            catch (InvalidValueException valueExc)
            {
                throw new IllegalArgumentException(
                    "Value for map operation violates validity contraints",
                    valueExc);
            }
            catch (SQLException sqlExc)
            {
                throw new LinStorSqlRuntimeException(
                    "Failed to add or update entries in the properties container " + instanceName,
                    sqlExc
                );
            }
        }

        @Override
        public void clear()
        {
            try
            {
                container.clear();
            }
            catch (SQLException sqlExc)
            {
                throw new LinStorSqlRuntimeException(
                    "Failed to clear the properties container " + instanceName,
                    sqlExc
                );
            }
        }

        @Override
        public Set<String> keySet()
        {
            return new KeySet(container);
        }

        @Override
        public Collection<String> values()
        {
            return new ValuesCollection(container);
        }

        @Override
        public Set<Entry<String, String>> entrySet()
        {
            return new PropsContainer.EntrySet(container);
        }

        @Override
        public int hashCode()
        {
            return entrySet().hashCode();
        }

        @Override
        public boolean equals(Object obj)
        {
            boolean equals = this == obj;
            if (!equals && obj != null && obj instanceof Map)
            {
                Map<?, ?> map = (Map<?, ?>) obj;
                equals = Objects.equals(map.entrySet(), this.entrySet());
            }
            return equals;
        }
    }

    abstract class BaseSet<T> implements Set<T>
    {
        PropsContainer container;

        BaseSet(PropsContainer con)
        {
            container = con;
        }

        @Override
        public int size()
        {
            return container.itemCount;
        }

        @Override
        public boolean isEmpty()
        {
            return container.itemCount == 0;
        }

        @Override
        public void clear()
        {
            try
            {
                container.clear();
            }
            catch (SQLException sqlExc)
            {
                throw new LinStorSqlRuntimeException(
                    "Failed to clear the properties container " +
                        instanceName,
                    sqlExc
                );
            }
        }

        @Override
        public int hashCode()
        {
            int hashCode = 0;
            for (Object value : this)
            {
                hashCode += value.hashCode();
            }
            return hashCode;
        }

        @Override
        public boolean equals(Object obj)
        {
            boolean equals = this == obj;
            if (!equals && obj != null && obj instanceof Set)
            {
                Set<?> set = (Set<?>) obj;
                equals = this.size() == set.size();
                Iterator<?> iterator = set.iterator();
                while (equals && iterator.hasNext())
                {
                    Object value = iterator.next();
                    equals = contains(value);
                }
            }
            return equals;
        }
    }

    class KeySet extends BaseSet<String>
    {
        KeySet(PropsContainer con)
        {
            super(con);
        }

        @Override
        public boolean contains(Object key)
        {
            return container.map().containsKey(key);
        }

        @Override
        public Iterator<String> iterator()
        {
            return new PropsContainer.KeysIterator(container);
        }

        @Override
        public Object[] toArray()
        {
            Set<String> pathList = new TreeSet<>(new PropsKeyComparator());
            container.collectAllKeys(container.getPath(), pathList, true);
            return pathList.toArray();
        }

        @Override
        public <T> T[] toArray(T[] keysArray)
        {
            Set<String> pathList = new TreeSet<>(new PropsKeyComparator());
            container.collectAllKeys(container.getPath(), pathList, true);
            return pathList.toArray(keysArray);
        }

        @Override
        public boolean add(String key)
        {
            boolean changed = false;
            try
            {
                String value = container.getProp(key, null);
                if (value == null)
                {
                    container.setProp(key, "", null);
                    changed = true;
                }
            }
            catch (InvalidKeyException keyExc)
            {
                throw new IllegalArgumentException(
                    "Key for map operation violates validity constraints",
                    keyExc
                );
            }
            catch (InvalidValueException valueExc)
            {
                throw new IllegalArgumentException(
                    "Value for map operation violates validity constraints",
                    valueExc
                );
            }
            catch (SQLException sqlExc)
            {
                throw new LinStorSqlRuntimeException(
                    "Failed to add or update entries in the properties container " +
                        instanceName,
                    sqlExc
                );
            }

            return changed;
        }

        @Override
        public boolean remove(Object key)
        {
            boolean changed = false;
            try
            {
                changed = container.removeProp((String) key) != null;
            }
            catch (ClassCastException castExc)
            {
                throw new ImplementationError(
                    "Key for map operation is of illegal object type",
                    castExc
                );
            }
            catch (InvalidKeyException keyExc)
            {
                throw new IllegalArgumentException(
                    "Key for map operation violates validity constraints",
                    keyExc
                );
            }
            catch (SQLException sqlExc)
            {
                throw new LinStorSqlRuntimeException(
                    "Failed to remove entries from the properties container " +
                        instanceName,
                    sqlExc
                );
            }

            return changed;
        }

        @Override
        public boolean containsAll(Collection<?> keyList)
        {
            boolean result = true;
            Map<String, String> conMap = container.map();
            for (Object key : keyList)
            {
                if (!conMap.containsKey(key))
                {
                    result = false;
                    break;
                }
            }
            return result;
        }

        @Override
        public boolean addAll(Collection<? extends String> keyList)
        {
            boolean changed = false;
            Map<String, String> entryMap = new TreeMap<>();
            for (String key : keyList)
            {
                entryMap.put(key, "");
                try
                {
                    changed = container.setAllProps(entryMap, null);
                }
                catch (InvalidKeyException keyExc)
                {
                    throw new IllegalArgumentException(
                        "Key for set operation violates validity constraints",
                        keyExc
                    );
                }
                catch (InvalidValueException valExc)
                {
                    throw new IllegalArgumentException(
                        "Value for set operation violates validity constraints",
                        valExc
                    );
                }
                catch (SQLException sqlExc)
                {
                    throw new LinStorSqlRuntimeException(
                        "Failed to add or update entries in the properties container " +
                            instanceName,
                        sqlExc
                    );
                }
            }
            return changed;
        }

        @Override
        public boolean retainAll(Collection<?> keyList)
        {
            boolean changed = false;
            // Collect all non-matching keys
            Set<String> removeSet = new TreeSet<>();
            for (String key : this)
            {
                if (!keyList.contains(key))
                {
                    removeSet.add(key);
                }
            }
            // Remove all entries with a non-matching key
            try
            {
                changed = container.removeAllProps(removeSet, null);
            }
            catch (SQLException sqlExc)
            {
                throw new LinStorSqlRuntimeException(
                    "Failed to remove entries from the properties container " +
                        instanceName,
                    sqlExc
                );
            }
            return changed;
        }

        @Override
        public boolean removeAll(Collection<?> keyList)
        {
            boolean changed = false;
            // Collect all matching keys
            Set<String> selection = new TreeSet<>();
            for (String key : this)
            {
                if (keyList.contains(key))
                {
                    selection.add(key);
                }
            }
            // Remove all entries with a matching key
            try
            {
                changed = container.removeAllProps(selection, null);
            }
            catch (SQLException sqlExc)
            {
                throw new LinStorSqlRuntimeException(
                    "Failed to remove entries from the properties container " +
                        instanceName,
                    sqlExc
                );
            }
            return changed;
        }
    }

    class EntrySet extends BaseSet<Map.Entry<String, String>>
    {
        EntrySet(PropsContainer con)
        {
            super(con);
        }

        @Override
        public boolean contains(Object obj)
        {
            Object key;
            if (obj instanceof String)
            {
                key = obj;
            }
            else
            if (obj instanceof Map.Entry)
            {
                key = ((Map.Entry<?, ?>) obj).getKey();
            }
            else
            {
                throw new IllegalArgumentException(
                    "Key must be either a String or an instance of Map.Entry<String, ?>"
                );
            }
            return container.map().containsKey(key);
        }

        @Override
        public Iterator<Map.Entry<String, String>> iterator()
        {
            return new PropsContainer.EntriesIterator(container);
        }

        @Override
        public Object[] toArray()
        {
            Map<String, String> collector = new TreeMap<>(new PropsKeyComparator());
            container.collectAllEntries(container.getPath(), collector, true);
            return collector.entrySet().toArray();
        }

        @Override
        public <T> T[] toArray(T[] entryArray)
        {
            Map<String, String> collector = new TreeMap<>(new PropsKeyComparator());
            container.collectAllEntries(container.getPath(), collector, true);
            return collector.entrySet().toArray(entryArray);
        }

        @Override
        public boolean add(Map.Entry<String, String> entry)
        {
            boolean changed = false;
            try
            {
                String key = entry.getKey();
                String value = entry.getValue();
                String oldValue = container.getProp(key, null);
                if (oldValue == null)
                {
                    container.setProp(key, value, null);
                    changed = true;
                }
            }
            catch (InvalidKeyException keyExc)
            {
                throw new IllegalArgumentException(
                    "Key for map operation violates validity constraints",
                    keyExc
                );
            }
            catch (InvalidValueException valueExc)
            {
                throw new IllegalArgumentException(
                    "Value for map operation violates validity constraints",
                    valueExc
                );
            }
            catch (SQLException sqlExc)
            {
                throw new LinStorSqlRuntimeException(
                    "Failed to add or update entries in the properties container " +
                        instanceName,
                    sqlExc
                );
            }
            return changed;
        }

        @Override
        public boolean remove(Object key)
        {
            return container.map().remove(key) != null;
        }

        @Override
        public boolean containsAll(Collection<?> keyList)
        {
            boolean result = true;
            for (Object key : keyList)
            {
                if (!container.map().containsKey(key))
                {
                    result = false;
                    break;
                }
            }
            return result;
        }

        @Override
        public boolean addAll(Collection<? extends Map.Entry<String, String>> collection)
        {
            boolean changed = false;
            Map<String, String> map = new HashMap<>();
            for (Map.Entry<String, String> entry : collection)
            {
                map.put(entry.getKey(), entry.getValue());
            }
            try
            {
                changed = container.setAllProps(map, null);
            }
            catch (InvalidKeyException | InvalidValueException exc)
            {
                throw new IllegalArgumentException(exc);
            }
            catch (SQLException sqlExc)
            {
                throw new LinStorSqlRuntimeException(
                    "Failed to add or update entries in the properties container " +
                        instanceName,
                    sqlExc
                );
           }
            return changed;
        }

        @Override
        public boolean retainAll(Collection<?> keyList)
        {
            boolean changed = false;
            // Collect all non-matching keys
            Set<String> removeSet = new TreeSet<>();
            for (Map.Entry<String, String> entry : this)
            {
                String key = entry.getKey();
                if (!keyList.contains(key))
                {
                    removeSet.add(key);
                }
            }
            // Remove all entries with a non-matching key
            try
            {
                changed = container.removeAllProps(removeSet, null);
            }
            catch (SQLException sqlExc)
            {
                throw new LinStorSqlRuntimeException(
                    "Failed to remove entries from the properties container " +
                        instanceName,
                    sqlExc
                );
            }
            return changed;
        }

        @Override
        public boolean removeAll(Collection<?> keyList)
        {
            boolean changed = false;
            // Collect all matching keys
            Set<String> selection = new TreeSet<>();
            for (Map.Entry<String, String> entry : this)
            {
                String key = entry.getKey();
                if (keyList.contains(key) || keyList.contains(entry))
                {
                    selection.add(key);
                }
            }
            // Remove all entries with a matching key
            try
            {
                changed = container.removeAllProps(selection, null);
            }
            catch (SQLException sqlExc)
            {
                throw new LinStorSqlRuntimeException(
                    "Failed to remove entries from the properties container " +
                        instanceName,
                    sqlExc
                );
            }
            return changed;
        }
    }

    class PropsKeyComparator implements Comparator<String>
    {
        @Override
        public int compare(String key1, String key2)
        {
            int result;
            int depth1 = key1.replaceAll("[^/]+", "").length();
            int depth2 = key2.replaceAll("[^/]+", "").length();
            if (depth1 != depth2)
            {
                result = Integer.compare(depth1, depth2);
            }
            else
            {
                result = key1.compareTo(key2);
            }
            return result;
        }
    }

    class ValuesCollection implements Collection<String>
    {
        private PropsContainer container;

        ValuesCollection(PropsContainer con)
        {
            container = con;
        }

        @Override
        public int size()
        {
            return container.itemCount;
        }

        @Override
        public boolean isEmpty()
        {
            return container.itemCount == 0;
        }

        @Override
        public boolean contains(Object value)
        {
            return container.map().containsValue(value);
        }

        @Override
        public Iterator<String> iterator()
        {
            return new PropsContainer.ValuesIterator(container);
        }

        @Override
        public Object[] toArray()
        {
            Collection<String> valuesList = new LinkedList<>();
            container.collectAllValues(valuesList, true);
            return valuesList.toArray();
        }

        @Override
        public <T> T[] toArray(T[] valuesArray)
        {
            Collection<String> valuesList = new LinkedList<>();
            container.collectAllValues(valuesList, true);
            return valuesList.toArray(valuesArray);
        }

        @Override
        public boolean add(String value)
        {
            throw new UnsupportedOperationException("Cannot add a value without a key");
        }

        @Override
        public boolean remove(Object value)
        {
            throw new UnsupportedOperationException("Cannot delete a value without a key");
        }

        @Override
        public boolean containsAll(Collection<?> valuesList)
        {
            boolean result = true;
            for (Object value : valuesList)
            {
                if (!contains(value))
                {
                    result = false;
                }
            }
            return result;
        }

        @Override
        public boolean addAll(Collection<? extends String> collObj)
        {
            throw new UnsupportedOperationException("Cannot add values without keys");
        }

        @Override
        public boolean removeAll(Collection<?> valueList)
        {
            boolean changed = false;
            // Collect the keys of all entries where the value matches
            Set<String> selection = new TreeSet<>();
            for (Map.Entry<String, String> entry : container.map().entrySet())
            {
                if (valueList.contains(entry.getValue()))
                {
                    selection.add(entry.getKey());
                }
            }
            // Remove all entries with a matching key
            try
            {
                changed = container.removeAllProps(selection, null);
            }
            catch (SQLException sqlExc)
            {
                throw new LinStorSqlRuntimeException(
                    "Failed to remove entries from the properties container " +
                        instanceName,
                    sqlExc
                );
            }
            return changed;
        }

        @Override
        public boolean retainAll(Collection<?> valueList)
        {
            boolean changed = false;
            // Collect the keys of all entries where the value matches
            Set<String> selection = new TreeSet<>();
            for (Map.Entry<String, String> entry : container.map().entrySet())
            {
                if (valueList.contains(entry.getValue()))
                {
                    selection.add(entry.getKey());
                }
            }
            // Remove all entries with a non-matching key
            try
            {
                changed = container.retainAllProps(selection, null);
            }
            catch (SQLException sqlExc)
            {
                throw new LinStorSqlRuntimeException(
                    "Failed to remove entries from the properties container " +
                        instanceName,
                    sqlExc
                );
            }
            return changed;
        }

        @Override
        public void clear()
        {
            try
            {
                container.clear();
            }
            catch (SQLException sqlExc)
            {
                throw new LinStorSqlRuntimeException(
                    "SQL exception",
                    "Failed to clear the properties container " +
                        instanceName,
                    sqlExc.getLocalizedMessage(),
                    null,
                    null,
                    sqlExc
                );
            }
        }

        @Override
        public boolean equals(Object other)
        {
            boolean equals = other != null && other instanceof Collection;
            if (equals)
            {
                Collection<?> otherCollection = (Collection<?>) other;
                equals &= this.containsAll(otherCollection);
                equals &= otherCollection.containsAll(this);
            }
            return equals;
        }

        @Override
        public int hashCode()
        {
            int hashCode = 0;
            for (Object value : this)
            {
                hashCode += value.hashCode();
            }
            return hashCode;
        }
    }

    private abstract class BaseIterator<T> implements Iterator<T>
    {
        PropsContainer container;
        private final Deque<Iterator<PropsContainer>> iterStack;
        private Iterator<PropsContainer> subConIter;
        Iterator<Map.Entry<String, String>> currentIter;
        String prefix;

        BaseIterator(PropsContainer con)
        {
            container = con;
            iterStack = new LinkedList<>();
            subConIter = container.containerMap.values().iterator();
            currentIter = container.propMap.entrySet().iterator();
        }

        @Override
        public boolean hasNext()
        {
            boolean hasNext = currentIter.hasNext();
            while (!hasNext)
            {
                // Try the next container
                PropsContainer subCon = recurseSubContainers();
                if (subCon != null)
                {
                    currentIter = subCon.propMap.entrySet().iterator();
                    prefix = subCon.getPath();
                }
                else
                {
                    // No more containers, stop
                    break;
                }
                hasNext = currentIter.hasNext();
            }
            return hasNext;
        }

        @Override
        public abstract T next();

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        PropsContainer recurseSubContainers()
        {
            PropsContainer subCon = null;
            while (subCon == null)
            {
                if (subConIter.hasNext())
                {
                    // Get the next sub container
                    subCon = subConIter.next();
                    // Put the current subcontainers iterator on the stack
                    // and get the subcontainer's iterator of subcontainers
                    iterStack.push(subConIter);
                    subConIter = subCon.containerMap.values().iterator();
                }
                else
                {
                    if (!iterStack.isEmpty())
                    {
                        subConIter = iterStack.pop();
                    }
                    else
                    {
                        // No more containers, stop
                        break;
                    }
                }
            }
            return subCon;
        }
    }

    private class EntriesIterator
    extends BaseIterator<Map.Entry<String, String>>
    {
        EntriesIterator(PropsContainer con)
        {
            super(con);
            prefix = container.getPath();
        }

        @Override
        public Map.Entry<String, String> next()
        {
            Map.Entry<String, String> entry = null;
            while (entry == null)
            {
                if (currentIter.hasNext())
                {
                    Map.Entry<String, String> localEntry = currentIter.next();
                    entry = new PropsConEntry(
                        container, prefix + localEntry.getKey(), localEntry.getValue()
                    );
                }
                else
                {
                    // Try the next container
                    PropsContainer subCon = recurseSubContainers();
                    if (subCon != null)
                    {
                        currentIter = subCon.propMap.entrySet().iterator();
                        prefix = subCon.getPath();
                    }
                    else
                    {
                        // No more containers, stop
                        break;
                    }
                }
            }
            return entry;
        }
    }

    private class KeysIterator
    extends BaseIterator<String>
    {
        KeysIterator(PropsContainer con)
        {
            super(con);
            prefix = container.getPath();
        }

        @Override
        public String next()
        {
            String key = null;
            while (key == null)
            {
                try
                {
                    key = prefix + currentIter.next().getKey();
                }
                catch (NoSuchElementException elemExc)
                {
                    // Try the next container
                    PropsContainer subCon = recurseSubContainers();
                    if (subCon != null)
                    {
                        currentIter = subCon.propMap.entrySet().iterator();
                        prefix = subCon.getPath();
                    }
                    else
                    {
                        // No more containers, stop
                        break;
                    }
                }
            }
            return key;
        }
    }

    private class ValuesIterator
    extends BaseIterator<String>
    {
        ValuesIterator(PropsContainer con)
        {
            super(con);
            prefix = container.getPath();
        }

        @Override
        public String next()
        {
            String value = null;
            while (value == null)
            {
                try
                {
                    value = currentIter.next().getValue();
                }
                catch (NoSuchElementException elemExc)
                {
                    // Try the next container
                    PropsContainer subCon = recurseSubContainers();
                    if (subCon != null)
                    {
                        currentIter = subCon.propMap.entrySet().iterator();
                        prefix = subCon.getPath();
                    }
                    else
                    {
                        // No more containers, stop
                        break;
                    }
                }
            }
            return value;
        }
    }

    private class PropsConEntry implements Map.Entry<String, String>
    {
        PropsContainer container;
        String entryKey;
        String entryValue;

        PropsConEntry(PropsContainer con, String key, String value)
        {
            container = con;
            entryKey = key;
            entryValue = value;
        }
        @Override
        public String getKey()
        {
            return entryKey;
        }

        @Override
        public String getValue()
        {
            return entryValue;
        }

        @Override
        public String setValue(String value)
        {
            String oldValue = entryValue;
            try
            {
                container.setProp(entryKey, value);
                entryValue = value;
            }
            catch (InvalidKeyException keyExc)
            {
                throw new ImplementationError(
                    "Container reports invalid key for a key that the container returned",
                    keyExc
                );
            }
            catch (InvalidValueException valueExc)
            {
                throw new IllegalArgumentException(
                    "Value for map operation violates validity constraints",
                    valueExc
                );
            }
            catch (SQLException sqlExc)
            {
                throw new LinStorSqlRuntimeException(
                    "Failed to add or update entries in the properties container " +
                        container.instanceName,
                    sqlExc
                );
            }
            return oldValue;
        }

        @Override
        public int hashCode()
        {
            // copied from JavaDoc of Map.Entry#hashCode()
            return (entryKey == null ? 0 : entryKey.hashCode()) ^
                (entryValue == null ? 0 : entryValue.hashCode());
        }

        @Override
        public boolean equals(Object obj)
        {
            boolean equals = this == obj;
            if (!equals && obj != null && obj instanceof Map.Entry)
            {
                Entry<?, ?> entry = (Entry<?, ?>) obj;
                equals = Objects.equals(entry.getKey(), entryKey);
                equals &= Objects.equals(entry.getValue(), entryValue);
            }
            return equals;
        }
    }

    public static void main(String[] args) throws SQLException
    {
        Props rootCon = new PropsContainerFactory(null).getInstance(null, null);
        try
        {
            BufferedReader stdin = new BufferedReader(
                new InputStreamReader(System.in)
            );
            String line = "";
            do
            {
                System.out.print("PropsContainer ==> ");
                System.out.flush();
                line = stdin.readLine();
                if (line != null)
                {
                    boolean haveCommand = false;
                    StringTokenizer tokens = new StringTokenizer(line);
                    try
                    {
                        String command = tokens.nextToken();
                        haveCommand = true;
                        switch (command)
                        {
                            case "set":
                                {
                                    String key = tokens.nextToken();
                                    String value = tokens.nextToken();
                                    System.out.printf("%s %s %s\n", command.toUpperCase(), key, value);
                                    rootCon.setProp(key, value);
                                }
                                break;
                            case "get":
                                {
                                    String key = tokens.nextToken();
                                    System.out.printf("%s %s\n", command.toUpperCase(), key);
                                    System.out.printf("  %s\n", rootCon.getProp(key));
                                }
                                break;
                            case "del":
                                {
                                    String key = tokens.nextToken();
                                    System.out.printf("%s %s\n", command.toUpperCase(), key);
                                    if (rootCon.removeProp(key) == null)
                                    {
                                        System.out.println("  Nonexistent property");
                                    }
                                    else
                                    {
                                        System.out.println("  Property deleted");
                                    }
                                }
                                break;
                            case "size":
                                {
                                    String key = tokens.nextToken();
                                    System.out.printf("%s %s\n", command.toUpperCase(), key);
                                    Props subCon = rootCon.getNamespace(key);
                                    if (subCon != null)
                                    {
                                        System.out.printf("  %s size = %d\n", subCon.getPath(), subCon.size());
                                    }
                                    else
                                    {
                                        System.out.println("Path is not a container");
                                    }
                                }
                                break;
                            case "clear":
                                {
                                    String key = tokens.nextToken();
                                    System.out.printf("%s %s\n", command.toUpperCase(), key);
                                    Props subCon = rootCon.getNamespace(key);
                                    if (subCon != null)
                                    {
                                        subCon.clear();
                                    }
                                    else
                                    {
                                        System.out.println("Path is not a container");
                                    }
                                }
                                break;
                            case "debug":
                                {
                                    String key = tokens.nextToken();
                                    System.out.printf("%s %s\n", command.toUpperCase(), key);
                                    Props con = rootCon.getNamespace(key);
                                    if (con != null)
                                    {
                                        Iterator<Map.Entry<String, String>> pIter =
                                            ((PropsContainer) con).iterateProps();
                                        while (pIter.hasNext())
                                        {
                                            Map.Entry<String, String> entry = pIter.next();
                                            System.out.printf(
                                                "  @ENTRY     %-30s: %s\n",
                                                entry.getKey(), entry.getValue()
                                            );
                                        }
                                        Iterator<PropsContainer> cIter =
                                            ((PropsContainer) con).iterateContainers();
                                        while (cIter.hasNext())
                                        {
                                            PropsContainer subCon = cIter.next();
                                            System.out.printf(
                                                "  @CONTAINER %-30s, size = %d\n",
                                                subCon.getPath(),
                                                subCon.size()
                                            );
                                        }
                                        System.out.printf(
                                            "End of list: Container '%s' size = %d\n",
                                            con.getPath(),
                                            con.size()
                                        );
                                    }
                                    else
                                    {
                                        System.out.println("Path is not a container");
                                    }
                                }
                                break;
                            case "list":
                                {
                                    String key = tokens.nextToken();
                                    System.out.printf("%s %s\n", command.toUpperCase(), key);
                                    Props con = rootCon.getNamespace(key);
                                    if (con != null)
                                    {
                                        Iterator<Map.Entry<String, String>> iter =
                                            con.iterator();
                                        while (iter.hasNext())
                                        {
                                            Map.Entry<String, String> entry = iter.next();
                                            System.out.printf(
                                                "  @ENTRY %-30s: %s\n",
                                                entry.getKey(), entry.getValue()
                                            );
                                        }
                                        System.out.printf(
                                            "End of list: Container '%s' size = %d\n",
                                            con.getPath(),
                                            con.size()
                                        );
                                    }
                                    else
                                    {
                                        System.out.println("Path is not a container");
                                    }
                                }
                                break;
                            case "keys":
                                {
                                    String key = tokens.nextToken();
                                    System.out.printf("%s %s\n", command.toUpperCase(), key);
                                    Props con = rootCon.getNamespace(key);
                                    if (con != null)
                                    {
                                        Iterator<String> iter = con.keysIterator();
                                        while (iter.hasNext())
                                        {
                                            String conkey = iter.next();
                                            System.out.printf("  @KEY %-30s\n", conkey);
                                        }
                                        System.out.printf(
                                            "End of list: Container '%s' size = %d\n",
                                            con.getPath(),
                                            con.size()
                                        );
                                    }
                                    else
                                    {
                                        System.out.println("Path is not a container");
                                    }
                                }
                                break;
                            case "values":
                                {
                                    String key = tokens.nextToken();
                                    System.out.printf("%s %s\n", command.toUpperCase(), key);
                                    Props con = rootCon.getNamespace(key);
                                    if (con != null)
                                    {
                                        Iterator<String> iter = con.valuesIterator();
                                        while (iter.hasNext())
                                        {
                                            String value = iter.next();
                                            System.out.printf("  @VALUE %s\n", value);
                                        }
                                        System.out.printf(
                                            "End of list: Container '%s' size = %d\n",
                                            con.getPath(),
                                            con.size()
                                        );
                                    }
                                    else
                                    {
                                        System.out.println("Path is not a container");
                                    }
                                }
                                break;
                            case "exit":
                                line = null;
                                break;
                            default:
                                System.out.printf("Unknown command '%s'\n", command);
                                break;
                        }
                    }
                    catch (NoSuchElementException elemExc)
                    {
                        if (haveCommand)
                        {
                            System.err.println("** ERROR: Missing argument");
                        }
                    }
                    catch (InvalidKeyException keyExc)
                    {
                        System.err.println("** ERROR: Invalid key");
                    }
                    catch (InvalidValueException valExc)
                    {
                        System.err.println("** ERROR: Invalid value");
                    }
                    catch (Exception exc)
                    {
                        System.err.println("** ERROR: Caught exception:");
                        System.err.printf("  " + exc.getMessage());
                        System.err.println("** Exception stack trace:");
                        exc.printStackTrace(System.err);
                        System.err.println("** End of exception report");
                    }
                }
            }
            while (line != null);
        }
        catch (IOException ioExc)
        {
            System.err.println(ioExc.getMessage());
        }
    }

    /**
     * PropsCon-path for StorPoolData
     */
    public static String buildPath(StorPoolName storPoolName, NodeName nodeName)
    {
        return PATH_STOR_POOL + nodeName.value +
            PATH_SEPARATOR + storPoolName.value;
    }

    /**
     * PropsCon-path for StorPoolDefinitionData
     */
    public static String buildPath(StorPoolName storPoolName)
    {
        return PATH_STOR_POOL_DFN + storPoolName.value;
    }

    /**
     * PropsCon-path for NodeData
     */
    public static String buildPath(NodeName nodeName)
    {
        return PATH_NODES + nodeName.value;
    }

    /**
     * PropsCon-path for ResourceDefinitionData
     */
    public static String buildPath(ResourceName resName)
    {
        return PATH_RESOURCE_DEFINITIONS + resName.value;
    }

    /**
     * PropsCon-path for ResourceData
     */
    public static String buildPath(NodeName nodeName, ResourceName resName)
    {
        return PATH_RESOURCES + nodeName.value +
            PATH_SEPARATOR + resName.value;
    }

    /**
     * PropsCon-path for VolumeDefinitionData
     */
    public static String buildPath(ResourceName resName, VolumeNumber volNr)
    {
        return PATH_VOLUME_DEFINITIONS + resName.value +
            PATH_SEPARATOR + volNr.value;
    }

    /**
     * PropsCon-path for VolumeData
     */
    public static String buildPath(NodeName nodeName, ResourceName resName, VolumeNumber volNr)
    {
        return PATH_VOLUMES + nodeName.value +
            PATH_SEPARATOR + resName.value +
            PATH_SEPARATOR + volNr.value;
    }

    /**
     * PropsCon-Path for NodeConnectionData
     */
    public static String buildPath(NodeName sourceName, NodeName targetName)
    {
        return PATH_NODE_CON_DEFINITIONS + sourceName.value +
            PATH_SEPARATOR + targetName.value;
    }

    /**
     * PropsCon-Path for ResourceConnectionData
     */
    public static String buildPath(NodeName sourceName, NodeName targetName, ResourceName resName)
    {
        return PATH_RESOURCE_CON_DEFINITIONS + sourceName.value +
            PATH_SEPARATOR + targetName.value +
            PATH_SEPARATOR + resName.value;
    }

    /**
     * PropsCon-Path for ResourceConnectionData
     */
    public static String buildPath(
        NodeName sourceName,
        NodeName targetName,
        ResourceName resName,
        VolumeNumber volNr
    )
    {
        return PATH_VOLUME_CON_DEFINITIONS + sourceName.value +
            PATH_SEPARATOR + targetName.value +
            PATH_SEPARATOR + resName.value +
            PATH_SEPARATOR + volNr.value;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        ArrayList<String> pairs = new ArrayList<>();
        for (Map.Entry<String, String> entry : this.map().entrySet())
        {
            StringBuilder sbPair = new StringBuilder();
            sbPair.append("  \"").append(entry.getKey()).append("\": \"").append(entry.getValue()).append('"');
            pairs.add(sbPair.toString());
        }
        sb.append(StringUtils.join(pairs, ",\n"));
        sb.append("\n}\n");
        return sb.toString();
    }
}
