using Dal;
using Quartz;
using Quartz.Impl;
using Quartz.Impl.Triggers;
using System;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using WebApplication1;

namespace MatchUnit.Common.SyncDatas
{
    /// <summary>
    /// 同步数据调度器管理类
    /// </summary>
    public class SyncManage
    {
        public static DataTable Marker { get; set; }

        /// <summary>
        /// mysql到mongodb的数据同步状态
        /// </summary>
        public static STATUS sync_status_db_str = STATUS.UnInit;

        /// <summary>
        /// mongodb同步到内存的str基因数据状态
        /// </summary>
        public static STATUS sync_status_cache_str = STATUS.UnInit;

        /// <summary>
        /// mongodb同步到内存的family基因数据状态
        /// </summary>
        public static STATUS sync_status_cache_family = STATUS.UnInit;

        /// <summary>
        /// 删除数据同步状态
        /// </summary>
        public static STATUS sync_status_del_manager = STATUS.UnInit;

        static IScheduler scheduler = null;

        public static void LoadMarker()
        {
            IDBHelper dBHelper = new DBHelperMySQL("", Program.Settings.DNALims);
            string marker = "select locus_type,locus_name,national_locus_name,alias,ord from locus_info  order by locus_type, ord";
            DataTable dtMarker = dBHelper.getDataTable(marker);
            if (dtMarker != null && dtMarker.Rows.Count > 0)
            {
                Marker = dtMarker;
            }
            dBHelper.closeConn();
        }

        /// <summary>
        /// 初始化job
        /// </summary>
        public static void InitJob(int interval)
        {
            //调度器
            Task<IScheduler> task = StdSchedulerFactory.GetDefaultScheduler();
            scheduler = task.Result;

            //job
            IJobDetail db_str = JobBuilder.Create<DBToDB_Str>().WithIdentity("db_str", "sync").Build();
            IJobDetail cache_str = JobBuilder.Create<DBToCache_Str>().WithIdentity("cache_str", "sync").Build();
            IJobDetail cache_family = JobBuilder.Create<DBToCache_Family>().WithIdentity("cache_family", "sync").Build();

            //触发器
            ITrigger db_str_trigger = TriggerBuilder.Create().WithIdentity("db_str_trigger","sync").WithCronSchedule($"0/{interval} * * * * ? ").Build();
            ITrigger cache_str_trigger = TriggerBuilder.Create().WithIdentity("cache_str_trigger", "sync").WithCronSchedule($"0/{interval} * * * * ? ").Build();
            ITrigger cache_family_trigger = TriggerBuilder.Create().WithIdentity("cache_family_trigger", "sync").WithCronSchedule($"0/{interval} * * * * ? ").Build();

            //一个job对应一个触发器
            scheduler.ScheduleJob(cache_family, cache_family_trigger);
            scheduler.ScheduleJob(cache_str, cache_str_trigger);
            scheduler.ScheduleJob(db_str, db_str_trigger);
        }

        /// <summary>
        /// 开始所有job
        /// </summary>
        public static void Start()
        {
            scheduler.Start();
        }

        /// <summary>
        /// 停止job
        /// name为空时，恢复所有job
        /// name不为空时，停止指定name的job
        /// </summary>
        /// <param name="name">job name</param>
        public static void Stop(string name="")
        {
            if (scheduler == null) return;
            if (string.IsNullOrEmpty(name))
                scheduler.PauseAll();
            else
                scheduler.PauseJob(new JobKey(name, "sync"));
        }

        /// <summary>
        /// 从停止状态中恢复job
        /// name为空时，恢复所有job
        /// name不为空时，恢复指定name的job
        /// </summary>
        /// <param name="name">job name</param>
        public static void Resume(string name="")
        {
            if (scheduler == null) return;
            if (string.IsNullOrEmpty(name))
                scheduler.ResumeAll();
            else
                scheduler.ResumeJob(new JobKey(name, "sync"));
        }

        /// <summary>
        /// 修改触发器间隔时间
        /// </summary>
        /// <param name="interval"></param>
        public static void ReTrigger(int interval)
        {
            if (scheduler == null) return;

            TriggerKey db_str_trigger = new TriggerKey("db_str_trigger", "sync");
            TriggerKey cache_family_trigger = new TriggerKey("cache_family_trigger", "sync");
            TriggerKey cache_str_trigger = new TriggerKey("cache_str_trigger", "sync");
            TriggerKey del_mg_tigger = new TriggerKey("del_mg_tigger", "sync");


            ICronTrigger db_str_trigger_cron = (ICronTrigger)scheduler.GetTrigger(db_str_trigger);
            ICronTrigger cache_family_trigger_cron = (ICronTrigger)scheduler.GetTrigger(cache_family_trigger);
            ICronTrigger cache_str_trigger_cron = (ICronTrigger)scheduler.GetTrigger(cache_str_trigger);
            ICronTrigger del_mg_tigger_cron = (ICronTrigger)scheduler.GetTrigger(del_mg_tigger);

            db_str_trigger_cron.CronExpressionString = $"0/{interval} * * * * ? ";
            cache_family_trigger_cron.CronExpressionString = $"0/{interval} * * * * ? ";
            cache_str_trigger_cron.CronExpressionString = $"0/{interval} * * * * ? ";
            del_mg_tigger_cron.CronExpressionString = $"0/{interval} * * * * ? ";

            scheduler.RescheduleJob(db_str_trigger, db_str_trigger_cron);
            scheduler.RescheduleJob(cache_family_trigger, cache_family_trigger_cron);
            scheduler.RescheduleJob(cache_str_trigger, cache_str_trigger_cron);
            scheduler.RescheduleJob(del_mg_tigger, del_mg_tigger_cron);
        }
    }
}
