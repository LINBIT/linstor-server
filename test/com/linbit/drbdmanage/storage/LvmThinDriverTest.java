package com.linbit.drbdmanage.storage;

import static com.linbit.drbdmanage.storage.LvmDriver.LVM_CHANGE_DEFAULT;
import static com.linbit.drbdmanage.storage.LvmDriver.LVM_VGS_DEFAULT;
import static com.linbit.drbdmanage.storage.LvmDriver.LVM_VOLUME_GROUP_DEFAULT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Map;

import org.junit.Test;

import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.utils.TestExtCmd.Command;
import com.linbit.extproc.utils.TestExtCmd.TestOutputData;

public class LvmThinDriverTest extends LvmDriverTest
{
    public LvmThinDriverTest()
    {
        super(new StorageTestUtils.DriverFactory()
        {
            @Override
            public StorageDriver createDriver(ExtCmd ec) throws StorageException
            {
                return new LvmThinDriver(ec);
            }
        });
    }

    public LvmThinDriverTest(DriverFactory driverFactory)
    {
        super(driverFactory);
    }

    @Override
    @Test
    public void testStartVolume() throws StorageException
    {
        final String identifier = "identifier";
        expectStartVolumeCommand(LVM_CHANGE_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, identifier, true);
        driver.startVolume(identifier);
    }

    @Override
    @Test(expected = StorageException.class)
    public void testStartUnknownVolume() throws StorageException
    {
        final String unknownIdentifier = "unknown";
        expectStartVolumeCommand(LVM_CHANGE_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, unknownIdentifier, false);
        driver.startVolume(unknownIdentifier);
    }

    @Override
    @Test
    public void testStopVolume() throws StorageException
    {
        final String identifier = "identifier";
        expectStopVolumeCommand(LVM_CHANGE_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, identifier, true);
        driver.stopVolume(identifier);
    }

    @Override
    @Test(expected = StorageException.class)
    public void testStopUnknownVolume() throws StorageException
    {
        final String unknownIdentifier = "unknown";
        expectStopVolumeCommand(LVM_CHANGE_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, unknownIdentifier, false);
        driver.stopVolume(unknownIdentifier);
    }


    @Override
    @Test
    public void testConfigurationKeys()
    {
        final HashSet<String> keys = new HashSet<>(driver.getConfigurationKeys());

        assertTrue(keys.remove(StorageConstants.CONFIG_LVM_CREATE_COMMAND_KEY));
        assertTrue(keys.remove(StorageConstants.CONFIG_LVM_REMOVE_COMMAND_KEY));
        assertTrue(keys.remove(StorageConstants.CONFIG_LVM_CHANGE_COMMAND_KEY));
        assertTrue(keys.remove(StorageConstants.CONFIG_LVM_CONVERT_COMMAND_KEY));
        assertTrue(keys.remove(StorageConstants.CONFIG_LVM_LVS_COMMAND_KEY));
        assertTrue(keys.remove(StorageConstants.CONFIG_LVM_VGS_COMMAND_KEY));
        assertTrue(keys.remove(StorageConstants.CONFIG_LVM_VOLUME_GROUP_KEY));
        assertTrue(keys.remove(StorageConstants.CONFIG_SIZE_ALIGN_TOLERANCE_KEY));
        assertTrue(keys.remove(StorageConstants.CONFIG_LVM_THIN_POOL_KEY));

        assertTrue(keys.isEmpty());
    }

    @Override
    @Test
    public void testTraits() throws StorageException
    {
        expectVgsExtentCommand(LVM_VGS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, 4096);
        Map<String, String> traits = driver.getTraits();

        final String traitProv = traits.get(DriverTraits.KEY_PROV);
        assertEquals(DriverTraits.PROV_THIN, traitProv);

        final String size = traits.get(DriverTraits.KEY_ALLOC_UNIT);
        assertEquals("4096", size);
    }


    @Override
    protected void expectLvmCreateVolumeBehavior(
        final String lvmCreateCommand,
        final long volumeSize,
        final String identifier,
        final String volumeGroup,
        final boolean volumeExists)
    {
        expectLvmCreateVolumeBehavior(
            lvmCreateCommand,
            volumeSize,
            identifier,
            LvmThinDriver.LVM_THIN_POOL_DEFAULT,
            volumeGroup,
            volumeExists);
    }


    protected void expectLvmCreateVolumeBehavior(
        final String lvmCreateCommand,
        final long volumeSize,
        final String identifier,
        final String thinPool,
        final String volumeGroup,
        final boolean volumeExists
    )
    {
        final Command cmd = new Command(
            lvmCreateCommand,
            "--virtualsize", volumeSize + "k",
            "--thinpool", thinPool,
            "--name", identifier,
            volumeGroup
            );

        final OutputData outData;
        if (volumeExists)
        {
            outData = new TestOutputData(
                "",
                "Logical volume \""+identifier+"\" already exists in volume group \""+volumeGroup+"\"",
                5);
        }
        else
        {
            outData = new TestOutputData(
                "Logical volume \"identifier\" created",
                "",
                0);
        }

        ec.setExpectedBehavior(cmd, outData);
    }
}
