using CacheModel.Model;
using Dal;
using GenoType2MDB.Match;
using Model.NetCore;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoLib;
using Quartz;
using RX.DNA.Match.MatchUnit.Genes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using WebApplication1;

namespace MatchUnit.Common.SyncDatas
{
    /// <summary>
    /// 同步mongodb到内存的str基因数据
    /// </summary>
    public class DBToCache_Str : IJob
    {
        public async Task Execute(IJobExecutionContext context)
        {
            SyncCommon.CacheSyncManage_Str();
        }
    }
}
