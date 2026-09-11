package com.linbit.linstor.api;

import com.linbit.linstor.api.pojo.SchedulePojo;
import com.linbit.linstor.api.pojo.backups.ScheduledRscsPojo;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.apicallhandler.controller.CtrlScheduleApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcher;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.ScheduleName;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Schedule;

import jakarta.inject.Inject;
import jakarta.inject.Provider;

import java.util.List;

import com.google.inject.testing.fieldbinder.Bind;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.assertj.core.api.Assertions.assertThat;

public class ScheduleApiTest extends ApiTestBase
{
    private static final String TEST_SCHED_NAME = "testSchedule";
    private static final String FULL_CRON = "0 4 * * *";
    private static final String INC_CRON = "0 * * * *";

    private static final long RC_SCHED_CREATED =
        ApiConsts.MASK_SCHEDULE | ApiConsts.MASK_CRT | ApiConsts.CREATED;
    private static final long RC_SCHED_MODIFIED =
        ApiConsts.MASK_SCHEDULE | ApiConsts.MASK_MOD | ApiConsts.MODIFIED;
    // the cron parsing errors of the create API are reported without a detail error code
    private static final long RC_SCHED_CRON_PARSE_ERROR =
        ApiConsts.MASK_SCHEDULE | ApiConsts.MASK_CRT | ApiConsts.MASK_ERROR;

    @Inject
    private Provider<CtrlScheduleApiCallHandler> scheduleApiCallHandlerProvider;

    @Bind
    @Mock
    protected FreeCapacityFetcher freeCapacityFetcher;

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        leaveScope();
    }

    @Test
    public void createScheduleSuccess() throws Exception
    {
        ApiCallRc rc = collect(
            scheduleApiCallHandlerProvider.get()
                .createSchedule(TEST_SCHED_NAME, FULL_CRON, INC_CRON, 3, 4, "RETRY", 5)
        );
        assertThat(rc).hasSize(1);
        expectRc(0, RC_SCHED_CREATED, rc.get(0));

        Schedule schedule = scheduleMap.get(new ScheduleName(TEST_SCHED_NAME));
        assertThat(schedule).isNotNull();
        assertThat(schedule.getFullCron().asString()).isEqualTo(FULL_CRON);
        assertThat(schedule.getIncCron()).isNotNull();
        assertThat(schedule.getIncCron().asString()).isEqualTo(INC_CRON);
        assertThat(schedule.getKeepLocal()).isEqualTo(3);
        assertThat(schedule.getKeepRemote()).isEqualTo(4);
        assertThat(schedule.getOnFailure()).isEqualTo(Schedule.OnFailure.RETRY);
        assertThat(schedule.getMaxRetries()).isEqualTo(5);
    }

    @Test
    public void createScheduleFullCronOnlyDefaultsToSkip() throws Exception
    {
        ApiCallRc rc = collect(
            scheduleApiCallHandlerProvider.get()
                .createSchedule(TEST_SCHED_NAME, FULL_CRON, null, null, null, null, null)
        );
        assertThat(rc).hasSize(1);
        expectRc(0, RC_SCHED_CREATED, rc.get(0));

        Schedule schedule = scheduleMap.get(new ScheduleName(TEST_SCHED_NAME));
        assertThat(schedule).isNotNull();
        assertThat(schedule.getIncCron()).isNull();
        assertThat(schedule.getKeepLocal()).isNull();
        assertThat(schedule.getKeepRemote()).isNull();
        assertThat(schedule.getOnFailure()).isEqualTo(Schedule.OnFailure.SKIP);
        assertThat(schedule.getMaxRetries()).isNull();
    }

    @Test
    public void createScheduleInvalidFullCronFails() throws Exception
    {
        ApiCallRc rc = collect(
            scheduleApiCallHandlerProvider.get()
                .createSchedule(TEST_SCHED_NAME, "not a cron expression", null, null, null, null, null)
        );
        assertThat(rc).hasSize(1);
        expectRc(0, RC_SCHED_CRON_PARSE_ERROR, rc.get(0));
        assertThat(scheduleMap.get(new ScheduleName(TEST_SCHED_NAME))).isNull();
    }

    @Test
    public void createScheduleInvalidIncCronFails() throws Exception
    {
        ApiCallRc rc = collect(
            scheduleApiCallHandlerProvider.get()
                .createSchedule(TEST_SCHED_NAME, FULL_CRON, "61 25 * * *", null, null, null, null)
        );
        assertThat(rc).hasSize(1);
        expectRc(0, RC_SCHED_CRON_PARSE_ERROR, rc.get(0));
        assertThat(scheduleMap.get(new ScheduleName(TEST_SCHED_NAME))).isNull();
    }

    @Test
    public void createScheduleWithoutFutureExecutionFails() throws Exception
    {
        // February 30th never exists, so the cron expression can never trigger
        ApiCallRc rc = collect(
            scheduleApiCallHandlerProvider.get()
                .createSchedule(TEST_SCHED_NAME, "0 0 30 2 *", null, null, null, null, null)
        );
        assertThat(rc).hasSize(1);
        expectRc(0, RC_SCHED_CRON_PARSE_ERROR, rc.get(0));
        assertThat(scheduleMap.get(new ScheduleName(TEST_SCHED_NAME))).isNull();
    }

    @Test
    public void createScheduleDuplicateNameFails() throws Exception
    {
        createDefaultSchedule();

        ApiCallRc rc = collect(
            scheduleApiCallHandlerProvider.get()
                .createSchedule(TEST_SCHED_NAME, FULL_CRON, null, null, null, null, null)
        );
        assertThat(rc).hasSize(1);
        expectRc(
            0,
            ApiConsts.MASK_SCHEDULE | ApiConsts.MASK_CRT | ApiConsts.FAIL_EXISTS_SCHEDULE,
            rc.get(0)
        );
    }

    @Test
    public void createScheduleInvalidNameFails() throws Exception
    {
        ApiCallRc rc = collect(
            scheduleApiCallHandlerProvider.get()
                .createSchedule("invalid name", FULL_CRON, null, null, null, null, null)
        );
        assertThat(rc).hasSize(1);
        expectRc(
            0,
            ApiConsts.MASK_SCHEDULE | ApiConsts.MASK_CRT | ApiConsts.FAIL_INVLD_SCHEDULE_NAME,
            rc.get(0)
        );
    }

    @Test
    public void createScheduleInvalidOnFailureFails() throws Exception
    {
        // an unknown on-failure value currently surfaces as an unhandled IllegalArgumentException
        ApiCallRc rc = collect(
            scheduleApiCallHandlerProvider.get()
                .createSchedule(TEST_SCHED_NAME, FULL_CRON, null, null, null, "explode", null)
        );
        assertThat(rc).hasSize(1);
        expectRc(
            0,
            ApiConsts.MASK_SCHEDULE | ApiConsts.MASK_CRT | ApiConsts.FAIL_UNKNOWN_ERROR,
            rc.get(0)
        );
        assertThat(scheduleMap.get(new ScheduleName(TEST_SCHED_NAME))).isNull();
    }

    @Test
    public void modifyScheduleSuccess() throws Exception
    {
        createDefaultSchedule();

        ApiCallRc rc = collect(
            scheduleApiCallHandlerProvider.get()
                .changeSchedule(TEST_SCHED_NAME, "30 2 * * 6", "30 * * * *", 5, 6, "RETRY", 7)
        );
        assertThat(rc).hasSize(1);
        expectRc(0, RC_SCHED_MODIFIED, rc.get(0));

        Schedule schedule = scheduleMap.get(new ScheduleName(TEST_SCHED_NAME));
        assertThat(schedule).isNotNull();
        assertThat(schedule.getFullCron().asString()).isEqualTo("30 2 * * 6");
        assertThat(schedule.getIncCron().asString()).isEqualTo("30 * * * *");
        assertThat(schedule.getKeepLocal()).isEqualTo(5);
        assertThat(schedule.getKeepRemote()).isEqualTo(6);
        assertThat(schedule.getOnFailure()).isEqualTo(Schedule.OnFailure.RETRY);
        assertThat(schedule.getMaxRetries()).isEqualTo(7);
    }

    @Test
    public void modifyScheduleClearOptionalSettings() throws Exception
    {
        createDefaultSchedule();

        // an empty inc-cron and negative keep / retry counts clear the corresponding settings
        ApiCallRc rc = collect(
            scheduleApiCallHandlerProvider.get()
                .changeSchedule(TEST_SCHED_NAME, null, "", -1, -1, null, -1)
        );
        assertThat(rc).hasSize(1);
        expectRc(0, RC_SCHED_MODIFIED, rc.get(0));

        Schedule schedule = scheduleMap.get(new ScheduleName(TEST_SCHED_NAME));
        assertThat(schedule).isNotNull();
        assertThat(schedule.getFullCron().asString()).isEqualTo(FULL_CRON);
        assertThat(schedule.getIncCron()).isNull();
        assertThat(schedule.getKeepLocal()).isNull();
        assertThat(schedule.getKeepRemote()).isNull();
        assertThat(schedule.getMaxRetries()).isNull();
    }

    @Test
    public void modifyUnknownScheduleFails() throws Exception
    {
        ApiCallRc rc = collect(
            scheduleApiCallHandlerProvider.get()
                .changeSchedule("unknownSchedule", FULL_CRON, null, null, null, null, null)
        );
        assertThat(rc).hasSize(1);
        expectRc(
            0,
            ApiConsts.MASK_SCHEDULE | ApiConsts.MASK_MOD | ApiConsts.FAIL_NOT_FOUND_SCHEDULE,
            rc.get(0)
        );
    }

    @Test
    public void modifyScheduleInvalidCronFails() throws Exception
    {
        createDefaultSchedule();

        // the ApiException thrown for an unparsable cron on modify is reported as unknown error
        ApiCallRc rc = collect(
            scheduleApiCallHandlerProvider.get()
                .changeSchedule(TEST_SCHED_NAME, "not a cron expression", null, null, null, null, null)
        );
        assertThat(rc).hasSize(1);
        expectRc(
            0,
            ApiConsts.MASK_SCHEDULE | ApiConsts.MASK_MOD | ApiConsts.FAIL_UNKNOWN_ERROR,
            rc.get(0)
        );

        Schedule schedule = scheduleMap.get(new ScheduleName(TEST_SCHED_NAME));
        assertThat(schedule).isNotNull();
        assertThat(schedule.getFullCron().asString()).isEqualTo(FULL_CRON);
    }

    @Test
    public void deleteScheduleSuccess() throws Exception
    {
        createDefaultSchedule();

        ApiCallRc rc = collect(scheduleApiCallHandlerProvider.get().delete(TEST_SCHED_NAME));
        assertThat(rc).hasSize(2);
        // "marked for deletion" of the outer delete operation
        expectRc(0, ApiConsts.MASK_SCHEDULE | ApiConsts.MASK_DEL | ApiConsts.DELETED, rc.get(0));
        // the actual deletion runs in a second transaction using a modify context
        expectRc(1, ApiConsts.MASK_SCHEDULE | ApiConsts.MASK_MOD | ApiConsts.DELETED, rc.get(1));

        assertThat(scheduleMap.get(new ScheduleName(TEST_SCHED_NAME))).isNull();
        assertThat(scheduleApiCallHandlerProvider.get().listSchedule()).isEmpty();
    }

    @Test
    public void deleteUnknownScheduleWarns() throws Exception
    {
        ApiCallRc rc = collect(scheduleApiCallHandlerProvider.get().delete("unknownSchedule"));
        assertThat(rc).hasSize(1);
        expectRc(
            0,
            ApiConsts.MASK_SCHEDULE | ApiConsts.MASK_DEL | ApiConsts.WARN_NOT_FOUND,
            rc.get(0)
        );
    }

    @Test
    public void listScheduleShowsCreatedSchedules() throws Exception
    {
        assertThat(scheduleApiCallHandlerProvider.get().listSchedule()).isEmpty();

        createDefaultSchedule();

        List<SchedulePojo> schedules = scheduleApiCallHandlerProvider.get().listSchedule();
        assertThat(schedules).hasSize(1);
        SchedulePojo pojo = schedules.get(0);
        assertThat(pojo.getScheduleName()).isEqualTo(TEST_SCHED_NAME);
        assertThat(pojo.getFullCron()).isEqualTo(FULL_CRON);
        assertThat(pojo.getIncCron()).isEqualTo(INC_CRON);
        assertThat(pojo.getKeepLocal()).isEqualTo(3);
        assertThat(pojo.getKeepRemote()).isEqualTo(4);
        assertThat(pojo.getOnFailure()).isEqualTo("RETRY");
        assertThat(pojo.getMaxRetries()).isEqualTo(5);
    }

    @Test
    public void listScheduledRscsShowsUnscheduledRscDfn() throws Exception
    {
        assertThat(scheduleApiCallHandlerProvider.get().listScheduledRscs(null, null, null, false)).isEmpty();

        enterScope();
        ResourceDefinition rscDfn = resourceDefinitionTestFactory.get("testRsc", true);
        rscDfnMap.put(new ResourceName("testRsc"), rscDfn);
        leaveScope();

        List<ScheduledRscsPojo> scheduledRscs = scheduleApiCallHandlerProvider.get()
            .listScheduledRscs(null, null, null, false);
        assertThat(scheduledRscs).hasSize(1);
        assertThat(scheduledRscs.get(0).rsc_name).isEqualToIgnoringCase("testRsc");
        assertThat(scheduledRscs.get(0).schedule).isNull();
        assertThat(scheduledRscs.get(0).reason).isEqualTo("none set");

        // resource definitions without schedule props are filtered out when only active entries are requested
        assertThat(scheduleApiCallHandlerProvider.get().listScheduledRscs(null, null, null, true)).isEmpty();
    }

    private void createDefaultSchedule() throws Exception
    {
        ApiCallRc rc = collect(
            scheduleApiCallHandlerProvider.get()
                .createSchedule(TEST_SCHED_NAME, FULL_CRON, INC_CRON, 3, 4, "RETRY", 5)
        );
        assertThat(rc).hasSize(1);
        expectRc(0, RC_SCHED_CREATED, rc.get(0));
    }
}
