package com.linbit.linstor.core.objects;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.pojo.SchedulePojo;
import com.linbit.linstor.core.identifier.ScheduleName;
import com.linbit.linstor.core.objects.remotes.AbsRemote.RemoteType;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.ScheduleDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ProtectedObject;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;

public class Schedule extends AbsCoreObj<Schedule> implements ProtectedObject
{
    public interface InitMaps
    {
        // currently only a place holder for future maps
    }

    private final ObjectProtection objProt;
    private final ScheduleDatabaseDriver driver;
    private final ScheduleName scheduleName;
    private final TransactionSimpleObject<Schedule, Cron> fullCron;
    private final TransactionSimpleObject<Schedule, Cron> incCron;
    private final TransactionSimpleObject<Schedule, Integer> keepLocal;
    private final TransactionSimpleObject<Schedule, Integer> keepRemote;
    private final TransactionSimpleObject<Schedule, OnFailure> onFailure;
    private final TransactionSimpleObject<Schedule, Integer> maxRetries;
    private final StateFlags<Flags> flags;
    private final CronParser parser;

    public Schedule(
        ObjectProtection objProtRef,
        UUID objIdRef,
        ScheduleDatabaseDriver driverRef,
        ScheduleName scheduleNameRef,
        long initialFlags,
        Cron fullCronRef,
        @Nullable Cron incCronRef,
        @Nullable Integer keepLocalRef,
        @Nullable Integer keepRemoteRef,
        OnFailure onFailureRef,
        @Nullable Integer maxRetriesRef,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProvider
    )
    {
        super(objIdRef, transObjFactory, transMgrProvider);
        objProt = objProtRef;
        scheduleName = scheduleNameRef;
        driver = driverRef;

        fullCron = transObjFactory.createTransactionSimpleObject(this, fullCronRef, driver.getFullCronDriver());
        incCron = transObjFactory.createTransactionSimpleObject(this, incCronRef, driver.getIncCronDriver());
        keepLocal = transObjFactory.createTransactionSimpleObject(this, keepLocalRef, driver.getKeepLocalDriver());
        keepRemote = transObjFactory.createTransactionSimpleObject(this, keepRemoteRef, driver.getKeepRemoteDriver());
        onFailure = transObjFactory.createTransactionSimpleObject(this, onFailureRef, driver.getOnFailureDriver());
        maxRetries = transObjFactory.createTransactionSimpleObject(this, maxRetriesRef, driver.getMaxRetriesDriver());

        flags = transObjFactory
            .createStateFlagsImpl(objProt, this, Flags.class, driver.getStateFlagsPersistence(), initialFlags);

        transObjs = Arrays.asList(
            objProt,
            fullCron,
            incCron,
            keepLocal,
            keepRemote,
            onFailure,
            flags,
            deleted
        );

        parser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX));
    }

    @Override
    public ObjectProtection getObjProt()
    {
        checkDeleted();
        return objProt;
    }

    @Override
    public int compareTo(Schedule schedule)
    {
        return scheduleName.compareTo(schedule.getName());
    }

    @Override
    public int hashCode()
    {
        checkDeleted();
        return Objects.hash(scheduleName);
    }

    @Override
    public boolean equals(Object obj)
    {
        checkDeleted();
        boolean ret = false;
        if (this == obj)
        {
            ret = true;
        }
        else if (obj instanceof Schedule)
        {
            Schedule other = (Schedule) obj;
            other.checkDeleted();
            ret = Objects.equals(scheduleName, other.scheduleName);
        }
        return ret;
    }

    public ScheduleName getName()
    {
        checkDeleted();
        return scheduleName;
    }

    public Cron getFullCron(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return fullCron.get();
    }

    public void setFullCron(AccessContext accCtx, String fullCronRef) throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        fullCron.set(parser.parse(fullCronRef));
    }

    public @Nullable Cron getIncCron(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return incCron.get();
    }

    public void setIncCron(AccessContext accCtx, @Nullable String incCronRef) throws AccessDeniedException,
        DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        if (incCronRef == null)
        {
            incCron.set(null);
        }
        else
        {
            incCron.set(parser.parse(incCronRef));
        }
    }

    public @Nullable Integer getKeepLocal(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return keepLocal.get();
    }

    public void setKeepLocal(AccessContext accCtx, @Nullable Integer keepLocalRef) throws AccessDeniedException,
        DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        keepLocal.set(keepLocalRef);
    }

    public @Nullable Integer getKeepRemote(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return keepRemote.get();
    }

    public void setKeepRemote(AccessContext accCtx, @Nullable Integer keepRemoteRef)
        throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        keepRemote.set(keepRemoteRef);
    }

    public OnFailure getOnFailure(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return onFailure.get();
    }

    public void setOnFailure(AccessContext accCtx, String onFailureRef)
        throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        onFailure.set(OnFailure.valueOfIgnoreCase(onFailureRef));
    }

    public @Nullable Integer getMaxRetries(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return maxRetries.get();
    }

    public void setMaxRetries(AccessContext accCtx, @Nullable Integer maxRetriesRef)
        throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        maxRetries.set(maxRetriesRef);
    }

    public StateFlags<Flags> getFlags()
    {
        checkDeleted();
        return flags;
    }

    public RemoteType getType()
    {
        checkDeleted();
        return RemoteType.S3;
    }

    public SchedulePojo getApiData(AccessContext accCtx, @Nullable Long fullSyncId, @Nullable Long updateId)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return new SchedulePojo(
            objId,
            scheduleName.displayValue,
            flags.getFlagsBits(accCtx),
            fullCron.get().asString(),
            incCron.get() == null ? null : incCron.get().asString(),
            keepLocal.get(),
            keepRemote.get(),
            onFailure.get().name(),
            maxRetries.get(),
            fullSyncId,
            updateId
        );
    }

    public void applyApiData(AccessContext accCtx, SchedulePojo apiData) throws AccessDeniedException,
        DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        fullCron.set(parser.parse(apiData.getFullCron()));
        if (apiData.getIncCron() != null)
        {
            incCron.set(parser.parse(apiData.getIncCron()));
        }
        else
        {
            incCron.set(null);
        }
        keepLocal.set(apiData.getKeepLocal());
        keepRemote.set(apiData.getKeepRemote());
        onFailure.set(OnFailure.valueOfIgnoreCase(apiData.getOnFailure()));
        maxRetries.set(apiData.getMaxRetries());

        flags.resetFlagsTo(accCtx, Flags.restoreFlags(apiData.getFlags()));
    }

    @Override
    public void delete(AccessContext accCtx) throws AccessDeniedException, DatabaseException
    {
        if (!deleted.get())
        {
            objProt.requireAccess(accCtx, AccessType.CONTROL);

            objProt.delete(accCtx);

            activateTransMgr();

            driver.delete(this);

            deleted.set(true);
        }
    }

    public enum Flags implements com.linbit.linstor.stateflags.Flags
    {
        DELETE(1L);

        public final long flagValue;

        Flags(long value)
        {
            flagValue = value;
        }

        @Override
        public long getFlagValue()
        {
            return flagValue;
        }

        public static Flags[] valuesOfIgnoreCase(String string)
        {
            Flags[] flags;
            if (string == null)
            {
                flags = new Flags[0];
            }
            else
            {
                String[] split = string.split(",");
                flags = new Flags[split.length];

                for (int idx = 0; idx < split.length; idx++)
                {
                    flags[idx] = Flags.valueOf(split[idx].toUpperCase().trim());
                }
            }
            return flags;
        }

        public static Flags[] restoreFlags(long scheduleFlags)
        {
            List<Flags> flagList = new ArrayList<>();
            for (Flags flag : Flags.values())
            {
                if ((scheduleFlags & flag.flagValue) == flag.flagValue)
                {
                    flagList.add(flag);
                }
            }
            return flagList.toArray(new Flags[flagList.size()]);
        }

        public static List<String> toStringList(long flagsMask)
        {
            return FlagsHelper.toStringList(Flags.class, flagsMask);
        }

        public static long fromStringList(List<String> listFlags)
        {
            return FlagsHelper.fromStringList(Flags.class, listFlags);
        }
    }

    public enum OnFailure
    {
        SKIP(1), RETRY(2);

        public final long value;

        private OnFailure(long valueRef)
        {
            value = valueRef;
        }

        public static @Nullable OnFailure getByValueOrNull(long valueRef)
        {
            OnFailure ret = null;
            for (OnFailure val : OnFailure.values())
            {
                if (val.value == valueRef)
                {
                    ret = val;
                    break;
                }
            }
            return ret;
        }

        public static OnFailure valueOfIgnoreCase(String string)
            throws IllegalArgumentException
        {
            return valueOf(string.toUpperCase());
        }

        public long getValue()
        {
            return value;
        }
    }

    @Override
    protected String toStringImpl()
    {
        return "Schedule '" + scheduleName + "'";
    }
}
