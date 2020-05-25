package com.linbit.linstor.layer.dmsetup.raid;

/**
 * TODO: implement RaidLayer
 *
 * ResourceNameSuffixes:
 *
 * (lvm) volumes will need to be suffixed from upper layers
 * eg: drbd, raid (with 3 disks), lvm
 * resource r0, 1 volume
 *
 * drbd
 *      resource name "r0"
 *      volume 0, rscNameSuffix ""
 *          devicePath: /dev/drbd1000
 *          backingDisk: /dev/md/r0
 * raid -> resource name "r0",
 *      volume 0, rscNameSuffix ""
 *          devicePath: /dev/md/r0
 *          backingDisk
 *              /dev/lvmpool/r0_00000_myraid_0
 *              /dev/lvmpool/r0_00000_myraid_1
 *              /dev/lvmpool/r0_00000_myraid_2
 * lvm -> resource name "r0",
 *      volume 0, rscNameSuffix "myraid_0"
 *          devicePath: /dev/lvmpool/r0_00000_myraid_0
 *      volume 0, rscNameSuffix "myraid_1"
 *          devicePath: /dev/lvmpool/r0_00000_myraid_1
 *      volume 0, rscNameSuffix "myraid_2"
 *          devicePath: /dev/lvmpool/r0_00000_myraid_2
 *
 * => every method handling one (or more) volumes need a Set
 * of Strings for every rscNameSuffix
 *
 *
 *
 *
 * Referenced StorPools
 *
 * RAID has to be configurable to put the backing volumes into different StorPools
 * => one Volume may have multiple StorPools
 * => storPools should not be stored within a Volume, but in its VolumeLayerData
 *
 *
 *
 * Multiple VolumeLayerData of same type
 *
 * RAID ontop of (different) LVM StorPools will require storing multiple LvmProvider
 * specific VolumeLayerData within the same volume
 * => DeviceLayerKind as a key for the layerStorageMap will not be sufficient anymore
 * ==> Map<Pair<DeviceLayerKind, String>, VlmLayerData> where String == rscNameSuffix ?
 *
 *
 *
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 *
 */
//public class RaidLayer implements DeviceLayer
//{
//    @Override
//    public String getName()
//    {
//        // TODO Auto-generated method stub
//        throw new ImplementationError("Not implemented yet");
//    }
//
//    @Override
//    public void prepare(Set<Resource> rscList, Set<Snapshot> affectedSnapshots)
//        throws StorageException, AccessDeniedException, SQLException
//    {
//        // TODO Auto-generated method stub
//        throw new ImplementationError("Not implemented yet");
//    }
//
//    @Override
//    public void updateGrossSize(Volume vlm) throws AccessDeniedException, SQLException
//    {
//        // TODO Auto-generated method stub
//        throw new ImplementationError("Not implemented yet");
//    }
//
//    @Override
//    public void process(
//        Resource rsc, RscLayerObject rscLayerData, String rscNameSuffix, Collection<Snapshot> snapshots,
//        ApiCallRcImpl apiCallRc
//    ) throws StorageException, ResourceException, VolumeException, AccessDeniedException, SQLException
//    {
//        // TODO Auto-generated method stub
//        throw new ImplementationError("Not implemented yet");
//    }
//
//    @Override
//    public void clearCache() throws StorageException
//    {
//        // TODO Auto-generated method stub
//        throw new ImplementationError("Not implemented yet");
//    }
//
//    @Override
//    public void setLocalNodeProps(Props localNodeProps)
//    {
//        // TODO Auto-generated method stub
//        throw new ImplementationError("Not implemented yet");
//    }
//
//
//}
