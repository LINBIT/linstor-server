package com.linbit.utils;

import com.linbit.linstor.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;

public class BidirectionalMultiMap<K, V>
{
    /*
     * Should these maps ever need a Collection instead of a Set, the remove* methods (and maybe others) need some
     * changes to make sure Lists (duplicate elements) can work as expected. This also means new tests will be needed.
     */
    private final Map<K, Set<V>> main;
    private final Map<V, Set<K>> inverted;
    private final Supplier<Set<K>> supplierK;
    private final Supplier<Set<V>> supplierV;

    public BidirectionalMultiMap()
    {
        this(new HashMap<>(), new HashMap<>(), () -> new TreeSet<>(), () -> new TreeSet<>());
    }

    @SuppressWarnings("unchecked")
    public <CK extends Set<K>, CV extends Set<V>> BidirectionalMultiMap(
        Map<K, CV> mainRef,
        Map<V, CK> invertedRef,
        Supplier<CK> supplierKRef,
        Supplier<CV> supplierVRef
    )
    {
        main = (Map<K, Set<V>>) mainRef;
        inverted = (Map<V, Set<K>>) invertedRef;
        supplierK = (Supplier<Set<K>>) supplierKRef;
        supplierV = (Supplier<Set<V>>) supplierVRef;
    }

    public int keyCount()
    {
        return main.size();
    }

    public int valueCount()
    {
        return inverted.size();
    }

    public boolean isEmpty()
    {
        return main.isEmpty();
    }

    public boolean containsKey(K key)
    {
        return main.containsKey(key);
    }

    public boolean containsValue(V value)
    {
        return inverted.containsKey(value);
    }

    public @Nullable Set<V> getByKey(K key)
    {
        return getByKeyOrDefault(key, null);
    }

    public @Nullable Set<K> getByValue(V value)
    {
        return getByValueOrDefault(value, null);
    }

    public Set<V> getByKeyOrEmpty(K key)
    {
        return getByKeyOrDefault(key, Collections.emptySet());
    }

    public Set<K> getByValueOrEmpty(V value)
    {
        return getByValueOrDefault(value, Collections.emptySet());
    }

    public @Nullable Set<V> getByKeyOrDefault(K key, @Nullable Set<V> dflt)
    {
        Set<V> ret = dflt;
        if (main.containsKey(key))
        {
            ret = Collections.unmodifiableSet(main.get(key));
        }
        return ret;
    }

    public @Nullable Set<K> getByValueOrDefault(V value, @Nullable Set<K> dflt)
    {
        Set<K> ret = dflt;
        if (inverted.containsKey(value))
        {
            ret = Collections.unmodifiableSet(inverted.get(value));
        }
        return ret;
    }

    public void add(K key, V value)
    {
        main.computeIfAbsent(key, k -> supplierV.get()).add(value);
        inverted.computeIfAbsent(value, v -> supplierK.get()).add(key);
    }

    public @Nullable Set<V> removeKey(K key)
    {
        Set<V> ret = main.remove(key);
        if (ret != null)
        {
            for (V val : ret)
            {
                Set<K> keys = inverted.get(val);
                if (keys != null)
                {
                    // this needs to remove all occurrences of key, make sure to change if Lists get allowed
                    keys.remove(key);
                    if (keys.isEmpty())
                    {
                        inverted.remove(val);
                    }
                }
            }
        }
        return ret;
    }

    public @Nullable Set<K> removeValue(V value)
    {
        Set<K> ret = inverted.remove(value);
        if (ret != null)
        {
            for (K key : ret)
            {
                Set<V> vals = main.get(key);
                if (vals != null)
                {
                    // this needs to remove all occurrences of value, make sure to change if Lists get allowed
                    vals.remove(value);
                    if (vals.isEmpty())
                    {
                        main.remove(key);
                    }
                }
            }
        }
        return ret;
    }

    public boolean remove(K key, V value)
    {
        Set<V> vals = main.get(key);
        boolean ret = false;
        if (vals != null)
        {
            ret = vals.remove(value);
            if (vals.isEmpty())
            {
                main.remove(key);
            }
        }
        Set<K> keys = inverted.get(value);
        if (keys != null)
        {
            keys.remove(key);
            if (keys.isEmpty())
            {
                inverted.remove(value);
            }
        }
        return ret;
    }

    public void clear()
    {
        main.clear();
        inverted.clear();
    }

    public Set<K> keySet()
    {
        return Collections.unmodifiableSet(main.keySet());
    }

    public Set<V> valueSet()
    {
        return Collections.unmodifiableSet(inverted.keySet());
    }

    public Set<Entry<K, Set<V>>> entrySet()
    {
        return Collections.unmodifiableSet(main.entrySet());
    }

    public Set<Entry<V, Set<K>>> entrySetInverted()
    {
        return Collections.unmodifiableSet(inverted.entrySet());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(inverted, main);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (!(obj instanceof BidirectionalMultiMap))
        {
            return false;
        }
        BidirectionalMultiMap<?, ?> other = (BidirectionalMultiMap<?, ?>) obj;
        return Objects.equals(inverted, other.inverted) && Objects.equals(main, other.main);
    }
}
