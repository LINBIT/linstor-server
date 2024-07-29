/*
 * Based on https://github.com/google/guice/wiki/CustomScopes
 *
 * @author Jesse Wilson
 * @author Fedor Karpelevitch
 */

package com.linbit.linstor.api;

import com.linbit.linstor.annotation.ErrorReporterContext;
import com.linbit.linstor.annotation.Nullable;

import javax.inject.Singleton;

import java.lang.annotation.Annotation;
import java.util.Map;

import com.google.common.collect.Maps;
import com.google.inject.Key;
import com.google.inject.OutOfScopeException;
import com.google.inject.Provider;
import com.google.inject.Scope;
import com.google.inject.Scopes;

import static com.google.common.base.Preconditions.checkState;

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
@Singleton
public class LinStorScope implements Scope
{
    private static final Provider<Object> SEEDED_KEY_PROVIDER =
        new Provider<>()
        {
            @Override
            public Object get()
            {
                throw new IllegalStateException("Not yet seeded");
            }
        };

    private final ThreadLocal<Map<Key<?>, Object>> values = new ThreadLocal<>();

    public ScopeAutoCloseable enter()
    {
        checkState(values.get() == null, "The current scope has already been entered");
        values.set(Maps.<Key<?>, Object>newHashMap());
        return this::exit;
    }

    private void exit()
    {
        checkState(values.get() != null, "There is no current scope to exit");
        values.remove();
    }

    public <T> void seed(Key<T> key, T value)
    {
        Map<Key<?>, Object> scopedObjects = getScopedObjectMap(key);
        checkState(!scopedObjects.containsKey(key),
            "A value for the key %s was already added to the current scope. Old value: %s New value: %s",
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
        return new Provider<>()
        {
            @SuppressWarnings("unchecked")
            @Override
            public T get()
            {
                Map<Key<?>, Object> scopedObjects = getScopedObjectMap(key);

                T current = null;
                if (scopedObjects != null)
                {
                    current = (T) scopedObjects.get(key);
                    if (current == null && !scopedObjects.containsKey(key))
                    {
                        current = unscoped.get();

                        // don't remember proxies; these exist only to serve circular dependencies
                        if (!Scopes.isCircularProxy(current))
                        {
                            scopedObjects.put(key, current);
                        }
                    }
                }
                return current;
            }
        };
    }

    private <T> @Nullable Map<Key<?>, Object> getScopedObjectMap(Key<T> key)
    {
        @Nullable Map<Key<?>, Object> scopedObjects = values.get();
        if (scopedObjects == null)
        {
            Annotation annotation = key.getAnnotation();
            Class<? extends Annotation> annotationType = key.getAnnotationType();

            boolean isErrorReporterContext = annotation != null && (annotation instanceof ErrorReporterContext);
            isErrorReporterContext |= annotationType != null && annotationType.equals(ErrorReporterContext.class);
            if (!isErrorReporterContext)
            {
                throw new OutOfScopeException(
                    "Cannot access " + key + " outside of a scoping block"
                );
            }
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

    public interface ScopeAutoCloseable extends AutoCloseable
    {
        @Override
        void close();
    }

    public boolean isEntered()
    {
        return values.get() != null;
    }

    public <T> boolean isSeeded(Key<T> key)
    {
        boolean ret = false;
        Map<Key<?>, Object> scopedObjectMap = getScopedObjectMap(key);
        if (scopedObjectMap != null)
        {
            ret = scopedObjectMap.containsKey(key);
        }
        return ret;
    }
}
