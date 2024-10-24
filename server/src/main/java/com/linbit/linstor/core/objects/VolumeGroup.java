package com.linbit.linstor.core.objects;

import com.linbit.linstor.api.pojo.VlmGrpPojo;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.apis.VolumeGroupApi;
import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.VolumeGroupDatabaseDriver;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ProtectedObject;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class VolumeGroup extends AbsCoreObj<VolumeGroup> implements ProtectedObject
{
    private final ResourceGroup rscGrp;

    private final VolumeNumber vlmNr;

    private final Props vlmGrpProps;

    private final StateFlags<VolumeGroup.Flags> flags;

    private final VolumeGroupDatabaseDriver dbDriver;

    private final Key vlmGrpKey;

    public VolumeGroup(
        UUID uuidRef,
        ResourceGroup rscGrpRef,
        VolumeNumber vlmNrRef,
        long initFlags,
        VolumeGroupDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<? extends TransactionMgr> transMgrProviderRef
    )
        throws DatabaseException
    {
        super(uuidRef, transObjFactoryRef, transMgrProviderRef);

        vlmNr = vlmNrRef;
        rscGrp = rscGrpRef;

        dbDriver = dbDriverRef;
        vlmGrpKey = new Key(this);

        flags = transObjFactoryRef.createStateFlagsImpl(
            rscGrpRef.getObjProt(),
            this,
            VolumeGroup.Flags.class,
            this.dbDriver.getStateFlagsPersistence(),
            initFlags
        );

        vlmGrpProps = propsContainerFactoryRef.getInstance(
            PropsContainer.buildPath(rscGrp.getName(), vlmNr),
            toStringImpl(),
            LinStorObject.VLM_GRP
        );

        transObjs = Arrays.asList(
            rscGrp,
            flags,
            vlmGrpProps,
            deleted
        );

    }

    @Override
    public ObjectProtection getObjProt()
    {
        checkDeleted();
        return rscGrp.getObjProt();
    }

    public ResourceGroup getResourceGroup()
    {
        return rscGrp;
    }

    public VolumeNumber getVolumeNumber()
    {
        return vlmNr;
    }

    public Key getKey()
    {
        // no check deleted
        return vlmGrpKey;
    }

    public Props getProps(AccessContext accCtxRef) throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(accCtxRef, rscGrp.getObjProt(), vlmGrpProps);
    }

    public StateFlags<VolumeGroup.Flags> getFlags()
    {
        checkDeleted();
        return flags;
    }

    @Override
    public void delete(AccessContext accCtxRef) throws AccessDeniedException, DatabaseException
    {
        if (!deleted.get())
        {
            rscGrp.getObjProt().requireAccess(accCtxRef, AccessType.USE);

            rscGrp.deleteVolumeGroup(accCtxRef, vlmNr);
            vlmGrpProps.delete();
            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    @Override
    public int compareTo(VolumeGroup other)
    {
        checkDeleted();
        int result = rscGrp.getName().compareTo(other.getResourceGroup().getName());
        if (result == 0)
        {
            result = vlmNr.compareTo(other.getVolumeNumber());
        }
        return result;
    }

    @Override
    public int hashCode()
    {
        checkDeleted();
        return Objects.hash(rscGrp, vlmNr);
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
        else if (obj instanceof VolumeGroup)
        {
            VolumeGroup other = (VolumeGroup) obj;
            other.checkDeleted();
            ret = Objects.equals(rscGrp, other.rscGrp) && Objects.equals(vlmNr, other.vlmNr);
        }
        return ret;
    }

    public VolumeGroupApi getApiData(AccessContext accCtxRef) throws AccessDeniedException
    {
        checkDeleted();
        return new VlmGrpPojo(
            objId,
            vlmNr.value,
            Collections.unmodifiableMap(vlmGrpProps.map()),
            flags.getFlagsBits(accCtxRef)
        );
    }

    public enum Flags implements com.linbit.linstor.stateflags.Flags
    {
        GROSS_SIZE(1L);

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

        public static Flags[] restoreFlags(long vlmDfnFlags)
        {
            List<Flags> flagList = new ArrayList<>();
            for (Flags flag : Flags.values())
            {
                if ((vlmDfnFlags & flag.flagValue) == flag.flagValue)
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

    @Override
    protected String toStringImpl()
    {
        return "VolumeGroup '" + vlmNr + "' of ResourceGroup '" + vlmGrpKey.rscGrpName + "'";
    }

    /**
     * Identifies a nodeConnection.
     */
    public static class Key implements Comparable<Key>
    {
        private final ResourceGroupName rscGrpName;
        private final VolumeNumber vlmNr;

        public Key(VolumeGroup vlmGrp)
        {
            this(vlmGrp.getResourceGroup().getName(), vlmGrp.getVolumeNumber());
        }

        public Key(ResourceGroupName rscGrpNameRef, VolumeNumber vlmNrRef)
        {
            rscGrpName = rscGrpNameRef;
            vlmNr = vlmNrRef;
        }

        public ResourceGroupName getRscGrpName()
        {
            return rscGrpName;
        }

        public VolumeNumber getVlmNr()
        {
            return vlmNr;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(rscGrpName, vlmNr);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (!(obj instanceof Key))
            {
                return false;
            }
            Key other = (Key) obj;
            return Objects.equals(rscGrpName, other.rscGrpName) && Objects.equals(vlmNr, other.vlmNr);
        }

        @Override
        public int compareTo(Key other)
        {
            int eq = rscGrpName.compareTo(other.rscGrpName);
            if (eq == 0)
            {
                eq = vlmNr.compareTo(other.vlmNr);
            }
            return eq;
        }
    }
}
