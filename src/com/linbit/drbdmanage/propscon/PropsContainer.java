package com.linbit.drbdmanage.propscon;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;

import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.ValueOutOfRangeException;

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

    public static PropsContainer createRootContainer()
    {
        PropsContainer con = null;
        try
        {
            con = new PropsContainer(null, null);
        }
        catch (InvalidKeyException keyExc)
        {
            // If root container creation generates an InvalidKeyException,
            // that is always a bug in the implementation
            throw new ImplementationError(
                "Root container creation generated an exception",
                keyExc
                );
        }
        return con;
    }

    PropsContainer(String key, PropsContainer parent) throws InvalidKeyException
    {
        if (key == null && parent == null)
        {
            // Create root PropsContainer

            containerKey = "/";

            rootContainer = this;
            parentContainer = null;
        }
        else
        {
            // Create sub PropsContainer

            ErrorCheck.ctorNotNull(PropsContainer.class, String.class, key);

            checkKey(key);
            containerKey = key;

            rootContainer = parent.getRoot();
            parentContainer = parent;
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
     * TODO: javadoc
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
     * TODO: javadoc
     *
     * @param key
     * @param value
     * @param namespace
     * @return
     * @throws InvalidKeyException
     * @throws InvalidValueException
     */
    @Override
    public String setProp(String key, String value, String namespace)
        throws InvalidKeyException, InvalidValueException
    {
        if (value == null)
        {
            throw new InvalidValueException();
        }

        String[] pathElements = splitPath(namespace, key);
        checkKey(pathElements[PATH_KEY]);

        PropsContainer con = ensureNamespaceExists(pathElements[PATH_NAMESPACE]);
        String oldValue = con.propMap.put(pathElements[PATH_KEY], value);
        if (oldValue == null)
        {
            con.modifySize(1);
        }
        return oldValue;
    }

    /**
     * TODO: javadoc
     *
     * @param key
     * @param namespace
     * @return
     * @throws InvalidKeyException
     */
    @Override
    public String removeProp(String key, String namespace)
        throws InvalidKeyException
    {
        String value = null;
        String[] pathElements = splitPath(namespace, key);
        checkKey(pathElements[PATH_KEY]);

        PropsContainer con = findNamespace(pathElements[PATH_NAMESPACE]);
        if (con != null)
        {
            value = con.propMap.remove(pathElements[PATH_KEY]);
            if (value != null)
            {
                con.modifySize(-1);
                con.removeCleanup();
            }
        }
        return value;
    }

    /**
     * TODO: javadoc
     *
     * @param entryMap
     * @param namespace
     * @throws InvalidKeyException
     */
    public boolean setAllProps(Map<? extends String, ? extends String> entryMap, String namespace)
        throws InvalidKeyException
    {
        int itemCounter = 0;
        try
        {
            for (Map.Entry<? extends String, ? extends String> entry : entryMap.entrySet())
            {
                String key = entry.getKey();
                String value = entry.getValue();

                String[] pathElements = splitPath(namespace, key);
                checkKey(pathElements[PATH_KEY]);

                PropsContainer con = ensureNamespaceExists(pathElements[PATH_NAMESPACE]);
                if (con.propMap.put(pathElements[PATH_KEY], value) == null)
                {
                    con.modifySize(1);
                }
                ++itemCounter;
            }
        }
        catch (InvalidKeyException keyExc)
        {
            int rollbackCounter = 0;
            for (Map.Entry<? extends String, ? extends String> entry : entryMap.entrySet())
            {
                if (rollbackCounter >= itemCounter)
                {
                    break;
                }
                String key = entry.getKey();

                String[] pathElements = splitPath(namespace, key);
                checkKey(pathElements[PATH_KEY]);

                PropsContainer con = findNamespace(pathElements[PATH_NAMESPACE]);
                if (con != null)
                {
                    // FIXME: this removes entries that already existed and were overwritten
                    //        by an entry in entryMap.
                    //        Instead, it should roll back to the value that the entry had
                    //        before it was changed.
                    if (con.propMap.remove(pathElements[PATH_KEY]) != null)
                    {
                        con.modifySize(-1);
                        con.removeCleanup();
                    }
                }
                ++rollbackCounter;
            }
            throw keyExc;
        }
        return itemCounter != 0;
    }

    /**
     * TODO: javadoc
     *
     * @param selection
     * @param namespace
     */
    boolean removeAllProps(Set<String> selection, String namespace)
    {
        boolean changed = false;
        for (String key : selection)
        {
            try
            {
                String[] pathElements = splitPath(namespace, key);
                checkKey(pathElements[PATH_KEY]);

                PropsContainer con = findNamespace(pathElements[PATH_NAMESPACE]);
                if (con != null)
                {
                    String value = con.propMap.remove(pathElements[PATH_KEY]);
                    if (value != null)
                    {
                        con.modifySize(-1);
                        con.removeCleanup();
                        changed = true;
                    }
                }
            }
            catch (InvalidKeyException keyExc)
            {
            }
        }
        return changed;
    }

    /**
     * TODO: javadoc
     *
     * @param selection
     * @param namespace
     */
    boolean retainAllProps(Set<String> selection, String namespace)
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

    /**
     * TODO: javadoc
     */
    @Override
    public void clear()
    {
        containerMap.clear();
        propMap.clear();
        if (parentContainer != null)
        {
            parentContainer.modifySize(itemCount * -1);
        }
        removeCleanup();
        itemCount = 0;
    }

    /**
     * TODO: javadoc
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
     * TODO: javadoc
     *
     * @param key
     * @param value
     * @return
     * @throws InvalidKeyException
     * @throws InvalidValueException
     */
    @Override
    public String setProp(String key, String value)
        throws InvalidKeyException, InvalidValueException
    {
        return setProp(key, value, null);
    }

    /**
     * TODO: javadoc
     *
     * @param key
     * @return
     * @throws InvalidKeyException
     */
    @Override
    public String removeProp(String key)
        throws InvalidKeyException
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
        return propMap.entrySet().iterator();
    }

    /**
     * TODO: Experimental: non-recursive iterator over all namespace containers
     *
     * @param namespace
     * @return
     */
    Iterator<PropsContainer> iterateContainers()
    {
        return containerMap.values().iterator();
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
            throw new InvalidKeyException();
        }

        StringBuilder sanitizedPath = new StringBuilder();
        StringTokenizer tokens = new StringTokenizer(path, "/");

        if (path.startsWith("/") && !forceRelative)
        {
            sanitizedPath.append("/");
        }
        boolean empty = true;
        while (tokens.hasMoreTokens())
        {
            String item = tokens.nextToken();
            if (item.length() > 0)
            {
                if (!empty)
                {
                    sanitizedPath.append("/");
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
            throw new InvalidKeyException();
        }

        String safePath;
        if (namespace != null)
        {
            String safeNamespace = sanitizePath(namespace, false);

            // If a namespace was specified, the path is always relative to the namespace
            safePath = sanitizePath(path, true);

            if (safeNamespace.length() == 0 || safeNamespace.endsWith("/"))
            {
                safePath = safeNamespace + safePath;
            }
            else
            {
                safePath = safeNamespace + "/" + safePath;
            }
        }
        else
        {
            // No namespace specified, path can be relative or absolute
            safePath = sanitizePath(path, false);
        }

        String pathElements[] = new String[2];
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
            throw new InvalidKeyException();
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
     * Returns the PropsContainer that constitutes the specified namespace, or null if the namespace
     * does not exist
     *
     * @param namespace The name of the namespace that should be returned
     * @return The namespace's PropsContainer, or null, if the namespace does not exist
     * @throws InvalidKeyException If the namespace specification is invalid
     */
    private PropsContainer findNamespace(String namespace) throws InvalidKeyException
    {
        PropsContainer con = this;
        if (namespace != null)
        {
            if (namespace.startsWith("/"))
            {
                con = getRoot();
            }
            StringTokenizer tokens = new StringTokenizer(namespace, "/");
            while (tokens.hasMoreTokens() && con != null)
            {
                con = con.containerMap.get(tokens.nextToken());
            }
        }
        return con;
    }

    /**
     * Creates the path to the specified namespace if it does not exist already
     *
     * @param namespace Path to the namespace
     * @return PropsContainer that constitutes the specified namespace
     * @throws InvalidKeyException If the namespace path is invalid
     */
    private PropsContainer ensureNamespaceExists(String namespace) throws InvalidKeyException
    {
        PropsContainer con = this;
        try
        {
            if (namespace != null)
            {
                if (namespace.startsWith("/"))
                {
                    con = getRoot();
                }
                StringTokenizer tokens = new StringTokenizer(namespace, "/");
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
            pathComponents.append("/");
        }
        return pathComponents;
    }

    PropsContainer createSubContainer(String key, PropsContainer con) throws InvalidKeyException
    {
        return new PropsContainer(key, con);
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
        if (key.contains("/"))
        {
            throw new InvalidKeyException();
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
                subCon.collectAllKeys(prefix + subCon.containerKey + "/", collector, recursive);
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
                subCon.collectAllEntries(prefix + subCon.containerKey + "/", collector, recursive);
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

    static class PropsConMap implements Map<String, String>
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
            return oldValue;
        }

        @Override
        public String remove(Object key)
        {
            String value = null;
            try
            {
                value = container.removeProp((String) key, null);
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
        }

        @Override
        public void clear()
        {
            container.clear();
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
    }

    static abstract class BaseSet<T> implements Set<T>
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
            container.clear();
        }
    }

    static class KeySet extends BaseSet<String>
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
            Set<String> pathList = new TreeSet<>();
            container.collectAllKeys(container.getPath(), pathList, true);
            return pathList.toArray();
        }

        @Override
        public <T> T[] toArray(T[] keysArray)
        {
            Set<String> pathList = new TreeSet<>();
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
            Map<String, String> entryMap = new TreeMap();
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
            changed = container.removeAllProps(removeSet, null);
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
            changed = container.removeAllProps(selection, null);
            return changed;
        }
    }

    static class EntrySet extends BaseSet<Map.Entry<String, String>>
    {
        EntrySet(PropsContainer con)
        {
            super(con);
        }

        @Override
        public boolean contains(Object key)
        {
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
            Map<String, String> collector = new TreeMap<>();
            container.collectAllEntries(container.getPath(), collector, true);
            return collector.entrySet().toArray();
        }

        @Override
        public <T> T[] toArray(T[] keysArray)
        {
            Map<String, String> collector = new TreeMap<>();
            container.collectAllEntries(container.getPath(), collector, true);
            return collector.entrySet().toArray(keysArray);
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

        // FIXME: this must call an outer class' method that can be overridden
        //        to enable serial number update in a subclass
        @Override
        public boolean addAll(Collection<? extends Map.Entry<String, String>> c)
        {
            // TODO: implement
            throw new UnsupportedOperationException("Not supported yet.");
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
            changed = container.removeAllProps(removeSet, null);
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
                if (keyList.contains(key))
                {
                    selection.add(key);
                }
            }
            // Remove all entries with a matching key
            changed = container.removeAllProps(selection, null);
            return changed;
        }
    }

    static class ValuesCollection implements Collection<String>
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
            for (Map.Entry<String, String> entry : container.entrySet())
            {
                if (!valuesList.contains(entry.getValue()))
                {
                    result = false;
                }
            }
            return result;
        }

        @Override
        public boolean addAll(Collection<? extends String> c)
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
            changed = container.removeAllProps(selection, null);
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
            changed = container.retainAllProps(selection, null);
            return changed;
        }

        @Override
        public void clear()
        {
            container.clear();
        }

        @Override
        public boolean equals(Object other)
        {
            boolean equals = other != null && other instanceof Collection;
            if(equals)
            {
                Collection<String> otherCollection = (Collection<String>) other;
                equals &= this.containsAll(otherCollection);
                equals &= otherCollection.containsAll(this);
            }
            return equals;
        }

        @Override
        public int hashCode()
        {
            return container.hashCode();
        }
    }

    private abstract static class BaseIterator<T> implements Iterator<T>
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

    private static class EntriesIterator
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
                try
                {
                    Map.Entry<String, String> localEntry = currentIter.next();
                    entry = new PropsConEntry(
                        container, prefix + localEntry.getKey(), localEntry.getValue()
                        );
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
            return entry;
        }
    }

    private static class KeysIterator
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

    private static class ValuesIterator
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

    private static class PropsConEntry implements Map.Entry<String, String>
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

        /**
         * TODO: javadoc
         *
         * @param value
         * @return
         */
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
            return oldValue;
        }
    }

    public static void main(String[] args)
    {
        SerialPropsContainer serialCon = SerialPropsContainer.createRootContainer();
        SerialGenerator serialGen = serialCon.getSerialGenerator();
        Props rootCon = SerialPropsContainer.createRootContainer(serialGen);
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
                                        System.out.printf("  @ENTRY     %-30s: %s\n", entry.getKey(), entry.getValue());
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
                                    System.out.printf(
                                        "SerialGenerator serial = %d, SerialContainer serial = '%s'\n",
                                        serialGen.peekSerial(),
                                        serialCon.getProp(SerialGenerator.KEY_SERIAL)
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
                                    System.out.printf(
                                        "SerialGenerator serial = %d, SerialContainer serial = '%s'\n",
                                        serialGen.peekSerial(),
                                        serialCon.getProp(SerialGenerator.KEY_SERIAL)
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
                                    System.out.printf(
                                        "SerialGenerator serial = %d, SerialContainer serial = '%s'\n",
                                        serialGen.peekSerial(),
                                        serialCon.getProp(SerialGenerator.KEY_SERIAL)
                                        );
                                }
                                else
                                {
                                    System.out.println("Path is not a container");
                                }
                            }
                            break;
                            case "close":
                            {
                                serialGen.closeGeneration();
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
}
