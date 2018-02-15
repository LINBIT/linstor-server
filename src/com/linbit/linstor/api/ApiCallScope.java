/*
 * Based on https://github.com/google/guice/wiki/CustomScopes
 *
 * @author Jesse Wilson
 * @author Fedor Karpelevitch
 */

package com.linbit.linstor.api;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.Maps;
import com.google.inject.Key;
import com.google.inject.OutOfScopeException;
import com.google.inject.Provider;
import com.google.inject.Scope;
import com.google.inject.Scopes;

import java.util.Map;

/**
 * Holds objects which are scoped to a specific API call.
 *
 * The scope can be initialized with one or more seed values by calling
 * <code>seed(key, value)</code> before the injector will be called upon to
 * provide for this key. For each key that will be inserted, a corresponding
 * binding is required so that the injector knows that the key is available
 * in this scope.
 * <pre><code>
 *   bind(key)
 *       .toProvider(ApiCallScope.&lt;KeyClass&gt;seededKeyProvider())
 *       .in(ApiCallScoped.class);
 * </code></pre>
 */
public class ApiCallScope implements Scope
{
    private static final Provider<Object> SEEDED_KEY_PROVIDER =
        new Provider<Object>()
        {
            public Object get()
            {
                throw new IllegalStateException("Not yet seeded in API call scope");
            }
        };

    private final ThreadLocal<Map<Key<?>, Object>> values = new ThreadLocal<>();

    public void enter()
    {
        checkState(values.get() == null, "An API call is already in progress");
        values.set(Maps.<Key<?>, Object>newHashMap());
    }

    public void exit()
    {
        checkState(values.get() != null, "No API call in progress");
        values.remove();
    }

    public <T> void seed(Key<T> key, T value)
    {
        Map<Key<?>, Object> scopedObjects = getScopedObjectMap(key);
        checkState(!scopedObjects.containsKey(key),
            "A value for the key %s was already added to the API call scope. Old value: %s New value: %s",
            key, scopedObjects.get(key), value);
        scopedObjects.put(key, value);
    }

    public <T> void seed(Class<T> clazz, T value)
    {
        seed(Key.get(clazz), value);
    }

    @Override
    public <T> Provider<T> scope(final Key<T> key, final Provider<T> unscoped)
    {
        return new Provider<T>()
        {
            public T get()
            {
                Map<Key<?>, Object> scopedObjects = getScopedObjectMap(key);

                @SuppressWarnings("unchecked")
                T current = (T) scopedObjects.get(key);
                if (current == null && !scopedObjects.containsKey(key))
                {
                    current = unscoped.get();

                    // don't remember proxies; these exist only to serve circular dependencies
                    if (!Scopes.isCircularProxy(current))
                    {
                        scopedObjects.put(key, current);
                    }
                }
                return current;
            }
        };
    }

    private <T> Map<Key<?>, Object> getScopedObjectMap(Key<T> key)
    {
        Map<Key<?>, Object> scopedObjects = values.get();
        if (scopedObjects == null)
        {
            throw new OutOfScopeException("Cannot access " + key + " outside of a scoping block");
        }
        return scopedObjects;
    }

    /**
     * Returns a provider that always throws exception complaining that the object
     * in question was not available in the API call scope.
     *
     * @return typed provider
     */
    @SuppressWarnings("unchecked")
    public static <T> Provider<T> seededKeyProvider()
    {
        return (Provider<T>) SEEDED_KEY_PROVIDER;
    }
}
