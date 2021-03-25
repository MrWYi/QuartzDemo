using Dal;
using Model.NetCore;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoLib;
using Quartz;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using WebApplication1;

namespace MatchUnit.Common.SyncDatas
{
    /// <summary>
    /// 同步mysql到mongodb的geno数据
    /// </summary>
    public class DBToDB_Str : IJob
    {
        //static SyncLog log =new SyncLog("db");
        public async Task Execute(IJobExecutionContext context)
        {
            SyncCommon.DBSyncManage();
        }
    }
}
