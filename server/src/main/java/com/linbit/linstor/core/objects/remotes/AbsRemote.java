package com.linbit.linstor.core.objects.remotes;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.backupshipping.BackupConsts;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.objects.AbsCoreObj;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ProtectedObject;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo.Version;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public abstract class AbsRemote extends AbsCoreObj<AbsRemote> implements Comparable<AbsRemote>, ProtectedObject
{
    protected final ObjectProtection objProt;
    protected final RemoteName remoteName;

    protected AbsRemote(
        UUID uuidRef,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef,
        ObjectProtection objProtRef,
        RemoteName remoteNameRef
    )
    {
        super(uuidRef, transObjFactory, transMgrProviderRef);
        objProt = objProtRef;
        remoteName = remoteNameRef;
    }


    public RemoteName getName()
    {
        checkDeleted();
        return remoteName;
    }

    public abstract StateFlags<Flags> getFlags();

    @Override
    public ObjectProtection getObjProt()
    {
        checkDeleted();
        return objProt;
    }

    public abstract RemoteType getType();

    public enum Flags implements com.linbit.linstor.stateflags.Flags
    {
        DELETE(1L),
        S3_USE_PATH_STYLE(1L << 1),
        /**
         * do not start new shippings to this remote, and delete it as soon as all in-progress shippings are done.
         */
        MARK_DELETED(1L << 2);

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

        public static Flags[] restoreFlags(long remoteFlags)
        {
            List<Flags> flagList = new ArrayList<>();
            for (Flags flag : Flags.values())
            {
                if ((remoteFlags & flag.flagValue) == flag.flagValue)
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

    public enum RemoteType
    {
        S3(BackupConsts.S3_REQ_EXT_TOOLS, BackupConsts.S3_OPT_EXT_TOOLS),
        SATELLITE(null, null),
        LINSTOR(
            BackupConsts.L2L_REQ_EXT_TOOLS, BackupConsts.L2L_OPT_EXT_TOOLS
        ), // controller only, should never be sent to satellite
        EBS(null, null) // only used by special (EBS) satellite
        ;

        private final @Nullable Map<ExtTools, Version> requiredExtTools;
        private final @Nullable Map<ExtTools, Version> optionalExtTools;

        RemoteType(
            @Nullable Map<ExtTools, Version> requiredExtToolsRef,
            @Nullable Map<ExtTools, Version> optionalExtToolsRef
        )
        {
            requiredExtTools = requiredExtToolsRef;
            optionalExtTools = optionalExtToolsRef;
        }

        public @Nullable Map<ExtTools, Version> getRequiredExtTools()
        {
            return requiredExtTools;
        }

        public @Nullable Map<ExtTools, Version> getOptionalExtTools()
        {
            return optionalExtTools;
        }
    }
}
