package com.linbit.locks;

import com.linbit.ImplementationError;
import com.linbit.linstor.core.CoreModule;
import javax.inject.Inject;
import javax.inject.Named;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import com.google.inject.Singleton;

@Singleton
public class LockGuardFactory
{
    public interface LockGuardBuilder
    {
        LockGuardBuilder setDefer(boolean defer);

        LockGuardBuilder write(LockObj... lockObj);

        LockGuardBuilder read(LockObj... lockObj);

        LockGuardBuilder lock(ReadWriteLock readWriteLock, LockType lockType);

        LockGuard build();

        default LockGuard buildDeferred()
        {
            setDefer(true);
            return build();
        }
    }

    public enum LockObj
    {
        RECONFIGURATION,
        CTRL_CONFIG,
        NODES_MAP,
        RSC_DFN_MAP,
        STOR_POOL_DFN_MAP,
        PLUGIN_NAMESPACE
    }

    public enum LockType
    {
        READ, WRITE
    }

    private final List<ReadWriteLock> lockOrder;

    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock rscDfnMapLock;
    private final ReadWriteLock storPoolDfnMapLock;
    private final ReadWriteLock ctrlConfigLock;
    private final ReadWriteLock reconfigurationLock;

    @Inject
    public LockGuardFactory(
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef,
        @Named(CoreModule.CTRL_CONF_LOCK) ReadWriteLock ctrlConfigLockRef
    )
    {
        reconfigurationLock = reconfigurationLockRef;
        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        storPoolDfnMapLock = storPoolDfnMapLockRef;
        ctrlConfigLock = ctrlConfigLockRef;
        lockOrder = Collections.unmodifiableList(
            // order defined in server/src/main/java/com/linbit/linstor/@LOCK_ORDER
            Arrays.asList(
                reconfigurationLockRef,
                ctrlConfigLockRef,
                nodesMapLockRef,
                rscDfnMapLockRef,
                storPoolDfnMapLockRef
            )
        );
    }

    public LockGuardBuilder create()
    {
        return new LockGuardBuilderImpl();
    }

    public LockGuardBuilder createDeferred()
    {
        return new LockGuardBuilderImpl(true);
    }

    public LockGuard build(LockType type, LockObj... lockObjs)
    {
        return build(new LockGuardBuilderImpl(), type, lockObjs);
    }

    public LockGuard buildDeferred(LockType type, LockObj... lockObjs)
    {
        return build(new LockGuardBuilderImpl(true), type, lockObjs);
    }

    private LockGuard build(
        LockGuardBuilder lockGuardBuilder,
        LockType type,
        LockObj[] lockObjs
    )
    {
        LockGuardBuilder tmpLGB;
        if (type == LockType.READ)
        {
            tmpLGB = lockGuardBuilder.read(lockObjs);
        }
        else
        {
            tmpLGB = lockGuardBuilder.write(lockObjs);
        }
        return tmpLGB.build();
    }


    private ReadWriteLock getByLockObj(LockObj type)
    {
        ReadWriteLock lock;
        switch (type)
        {
            case RECONFIGURATION:
                lock = reconfigurationLock;
                break;
            case NODES_MAP:
                lock = nodesMapLock;
                break;
            case RSC_DFN_MAP:
                lock = rscDfnMapLock;
                break;
            case STOR_POOL_DFN_MAP:
                lock = storPoolDfnMapLock;
                break;
            case CTRL_CONFIG:
                lock = ctrlConfigLock;
                break;
            default:
                throw new ImplementationError("Unknown lock type: " + type);
        }
        return lock;
    }

    private int compareLocks(ReadWriteLock lock1, ReadWriteLock lock2)
    {
        return Integer.compare(
            lockOrder.indexOf(lock1),
            lockOrder.indexOf(lock2)
        );
    }

    private class LockGuardBuilderImpl implements LockGuardBuilder
    {
        private final TreeMap<ReadWriteLock, LockType> locks;

        private boolean defer = false;

        private LockGuardBuilderImpl()
        {
            locks = new TreeMap<>(LockGuardFactory.this::compareLocks);
        }

        private LockGuardBuilderImpl(boolean deferRef)
        {
            this();
            setDefer(deferRef);
        }

        @Override
        public LockGuardBuilder setDefer(boolean deferRef)
        {
            defer = deferRef;
            return this;
        }

        @Override
        public LockGuardBuilder read(LockObj... lockObjs)
        {
            for (LockObj lockObj : lockObjs)
            {
                lock(getByLockObj(lockObj), LockType.READ);
            }
            return this;
        }

        @Override
        public LockGuardBuilder write(LockObj... lockObjs)
        {
            for (LockObj lockObj : lockObjs)
            {
                lock(getByLockObj(lockObj), LockType.WRITE);
            }
            return this;
        }

        @Override
        public LockGuardBuilder lock(ReadWriteLock lock, LockType type)
        {
            locks.put(lock, type);
            return this;
        }

        @Override
        public LockGuard buildDeferred()
        {
            setDefer(true);
            return build();
        }

        @Override
        public LockGuard build()
        {
            if (!locks.isEmpty() && locks.get(reconfigurationLock) == null)
            {
                locks.put(reconfigurationLock, LockType.READ);
            }

            Lock[] lockArr = new Lock[locks.size()];
            int lockIdx = 0;
            for (Entry<ReadWriteLock, LockType> entry : locks.entrySet())
            {
                Lock lock;
                if (entry.getValue() == LockType.READ)
                {
                    lock = entry.getKey().readLock();
                }
                else
                {
                    lock = entry.getKey().readLock();
                }
                lockArr[lockIdx] = lock;
                ++lockIdx;
            }
            return new LockGuard(defer, lockArr);
        }
    }
}

