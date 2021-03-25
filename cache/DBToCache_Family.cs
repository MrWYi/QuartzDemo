using CacheModel.Model.ModelFamliy;
using Model.NetCore;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoLib;
using Quartz;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using WebApplication1;

namespace MatchUnit.Common.SyncDatas
{
    /// <summary>
    /// 同步mongodb到内存的家系基因数据
    /// </summary>
    public class DBToCache_Family : IJob
    {
        public async Task Execute(IJobExecutionContext context)
        {
            SyncCommon.CacheSyncManage_Family();
        }
    }
}
