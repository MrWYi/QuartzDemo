using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Dal;
using GenoType2MDB.Match;
using MatchUnit.Controllers;
using Model.NetCore;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoLib;
using MySql.Data.MySqlClient;
using RX.DNA.Match.MatchUnit.Genes;
using WebApplication1;
using Microsoft.AspNetCore.Mvc;
using Controllers;
using System.Text;
using Newtonsoft.Json;
using System.Threading;
using System.Diagnostics;
using CacheModel.Model.ModelFamliy;
using CacheModel.Model;
using Common;

namespace MatchUnit.Common
{
    /// <summary>
    ///(已废弃) 数据同步类
    ///修改为Quartz调度管理定时任务 2021/3/23
    /// </summary>
    public static class SyncData1
    {
        /// <summary>
        /// 数据同步用到的并发数量
        /// </summary>
        private static int GeneSyncThreadCount = 2;
        /// <summary>
        /// 数据同步记录耗时的list
        /// </summary>
        public static List<String> GeneSyncStopWatchList = new List<string>();
        /// <summary>
        /// 记录同步数据耗时
        /// static方便查看当前同步已消耗时长
        /// </summary>
        public static Stopwatch GeneSyncStopWatch = new Stopwatch();
        /// <summary>
        /// 普通基因同步删除状态锁
        /// </summary>
        public static int StrDeleteLock = 0;
        /// <summary>
        /// 家系基因同步删除状态锁
        /// </summary>
        public static int FamilyDeleteLock = 0;

        public static DataTable Marker = null;

        /// <summary>
        /// 初始化定时器
        /// </summary>
        public static void InitTimersMethod()
        {
            GetCurGeneInfo.Interval = 1000;
            GetCurGeneInfo.AutoReset = true;
            GetCurGeneInfo.Elapsed += GetCurGeneInfo_Elapsed;

            GetFamilyGeneInfo.Interval = 1000;
            GetFamilyGeneInfo.AutoReset = true;
            GetFamilyGeneInfo.Elapsed += GetFamilyGeneInfo_Elapsed;

            GetCurGeneInfo_MySQL.Interval = 1000;
            GetCurGeneInfo_MySQL.AutoReset = true;
            GetCurGeneInfo_MySQL.Elapsed += GetCurGeneInfo_MySQL_Elapsed; ;

            //改为str同步定时器维护家系基因的同步
            GetFamilyGeneInfo_MySQL.Interval = 1000;
            GetFamilyGeneInfo_MySQL.Enabled = true;
            GetFamilyGeneInfo_MySQL.AutoReset = true;
            GetFamilyGeneInfo_MySQL.Elapsed += GetFamilyGenoInfo_MySQL_Elapsed;
        }

        /// <summary>
        /// 停止定时器
        /// </summary>
        public static void StopTimers()
        {
            GetCurGeneInfo.Enabled = false;
            GetFamilyGeneInfo.Enabled = false;
            GetCurGeneInfo_MySQL.Enabled = false;
            //GetFamilyGeneInfo_MySQL.Enabled = false;
        }

        /// <summary>
        /// 启动定时器
        /// </summary>
        public static void StartTimers()
        {
            //GetCurGeneInfo.Enabled = true;
            //GetFamilyGeneInfo.Enabled = true;
            GetCurGeneInfo_MySQL.Enabled = true;
            //GetFamilyGeneInfo_MySQL.Enabled = true;
        }

        public static void LoadMarker()
        {
            IDBHelper dBHelper = new DBHelperMySQL("", Program.Settings.DNALims);
            string marker = "select locus_type,locus_name,national_locus_name,alias,ord from locus_info  order by locus_type, ord";
            DataTable dtMarker = dBHelper.getDataTable(marker);
            if (dtMarker!=null&&dtMarker.Rows.Count>0)
            {
                Marker = dtMarker;
            }
            dBHelper.closeConn();
        }
        public static void SetThreadCount(int count)
        {
            GeneSyncThreadCount = count;
        }
        public static int GetThreadCount()
        {
            return GeneSyncThreadCount;
        }

        #region mysql数据同步mongo

        /// <summary>
        /// 同步mysql的STR基因定时器
        /// </summary>
        private static readonly System.Timers.Timer GetCurGeneInfo_MySQL = new System.Timers.Timer();
        /// <summary>
        /// 同步mysql的家系基因定时器
        /// </summary>
        private static readonly System.Timers.Timer GetFamilyGeneInfo_MySQL = new System.Timers.Timer();

        /// <summary>
        /// mysql的常XY基因同步服务状态
        /// </summary>
        public static STATUS StrSyncServicesStatus_MySQL = STATUS.UnInit;
        /// <summary>
        /// mysql的家系基因同步服务状态
        /// </summary>
        public static STATUS FamilySyncServicesStatus_MySQL = STATUS.UnInit;

        /// <summary>
        /// mysql数据库中常XY基因数据总数量
        /// </summary>
        public static long STR_DB_AllCount_MySQL;
        /// <summary>
        /// mysql同步到MONGO中的常XY基因数量
        /// </summary>
        public static long STR_MONGO_AllCount_MySQL;

        /// <summary>
        /// mysql数据库中家系数据总数量
        /// </summary>
        public static long Family_DB_AllCount_MySQL;
        /// <summary>
        /// mysql同步到MONGO中的家系基因数量
        /// </summary>
        public static long Family_MONGO_AllCount_MySQL;

        /// <summary>
        /// 同步mysql到mongo的常str基因方法定时器
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void GetCurGeneInfo_MySQL_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            //if (StrSyncServicesStatus_MySQL == STATUS.UnInit || Program.engineStatus != STATUS.Idle) return;
            if (StrSyncServicesStatus_MySQL == STATUS.UnInit) return;
            if(StrDeleteLock==0)
                DeleteGeneAsync_MySQL();
            if (StrSyncServicesStatus_MySQL != STATUS.Idle)
            {
                //内存数据等同于mongo内已同步数据
                GetRAMCount();
                //检查是否有更新数据
                if (GetUpdateTask_MySQL() <= 0)
                {
                    if (STR_MONGO_AllCount_MySQL == STR_DB_AllCount_MySQL || StrSyncServicesStatus_MySQL == STATUS.Exiting)
                    {
                        StrSyncServicesStatus_MySQL = STATUS.Idle;
                        return;
                    }
                    if (StrSyncServicesStatus_MySQL == STATUS.Initing) return;
                }
                if (StrSyncServicesStatus_MySQL == STATUS.Initing) return;
            }
            StrSyncServicesStatus_MySQL = STATUS.Initing;
            
            //验证位点
            if (Marker==null||Marker.Rows.Count<=0) LoadMarker();

            //分页同步
            //状态，默认0 插入，1 删除，2 更新 
            int pageSize = Program.Settings.Limit;
            IDBHelper dBHelper = new DBHelperMySQL("", Program.Settings.DNALims);
            string maxIdSql = "SELECT max(id) FROM `comparedatatask` where ex8=0 and `status`<>1 ";
            var id = dBHelper.execScalar(maxIdSql);
            dBHelper.closeConn();
            if (id.Equals(DBNull.Value))
            {
                StrSyncServicesStatus_MySQL = STATUS.Exiting;
                return;
            }

            var count = Convert.ToInt32(id);
            int page = (count % pageSize == 0 ? count / pageSize : count / pageSize + 1);


            GeneSyncStopWatch = new Stopwatch();
            GeneSyncStopWatch.Start();
            try
            {
                //ParallelOptions parallelOptions = new ParallelOptions();
                //parallelOptions.MaxDegreeOfParallelism = GeneSyncThreadCount;
                //Parallel.For(0, page, parallelOptions, p =>
                //{
                //    SyncParalle(p, pageSize, Marker);
                //});
                for (int p = 0; p < page; p++)
                {
                    SyncParalle(p, pageSize, Marker);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            finally
            {
                StrSyncServicesStatus_MySQL = STATUS.Exiting;
            }

            GeneSyncStopWatch.Stop();
            GeneSyncStopWatchList.Add(GeneSyncStopWatch.ElapsedMilliseconds.ToString());
        }

        private static void SyncParalle(int pageIndex,int pageSize,DataTable dtMarker)
        {
            IDBHelper dBHelper = new DBHelperMySQL("", Program.Settings.DNALims);
            try {
                string sql = $"SELECT * from comparedatatask where id>={pageIndex * pageSize}  and id <{pageIndex * pageSize + pageSize} and ex8=0 and `status`<>1 and `type`=0 order by updatetime desc,`level`";
                var dtGene = dBHelper.getDataTable(sql);
                if (dtGene != null && dtGene.Rows.Count > 0)
                {
                    for (int i = 0; i < dtGene.Rows.Count; i++)
                    {
                        var id = dtGene.Rows[i]["id"].ToString();

                        var server = dtGene.Rows[i]["server"].ToString();
                        var geneIndexid = dtGene.Rows[i]["geneindexid"].ToString();

                        //string sqlindex = $"select* from geneindex where idper = (select idper from geneindex where id ={geneIndexid} and `server`= {server})and idsampling<>0 and deleted = 0 and `server`= {server}";


                        //string sqlindex = @"select g.id,g.server,g.idsampling,g.idper " + GenoWork.GenosSql + $"and g.id={geneIndexid} and g.`server`={server}";
                        string sqlindex = @"select g.id,g.server,g.idsampling,g.idper  from geneindex g,sampling s,per p,commitment c  
                                    where g.idsampling<> 0 and g.idper<> 0 and s.perid<> 0 and s.commitid<> 0 and p.category<>98 
                                    and g.deleted = 0 and p.deleted = 0 and c.deleted = 0 and s.deleted = 0 
                                    and c.`server`= g.`server` and p.`server`= g.`server`and s.`server`= g.`server` 
                                    and s.perid = p.id and s.id = g.idsampling and c.id = s.commitid and g.id='"+geneIndexid+"' and g.`server`='"+server+"'";
                        var dtIndex = dBHelper.getDataTable(sqlindex);
                        if (dtIndex.Rows.Count>0)
                        {
                            for (int k = 0; k < dtIndex.Rows.Count; k++)
                            {
                                var sampId = dtIndex.Rows[k]["idsampling"].ToString();
                                var perId = dtIndex.Rows[k]["idper"].ToString();

                                string sqlType = "select * from genotype where id =" + geneIndexid + " and `server`=" + server;
                                var limsGenedt = dBHelper.getDataTable(sqlType);
                                var drList = limsGenedt.Select("id=" + geneIndexid + " and server=" + server);
                                logWork.AddLog(DateTime.Now, "", sqlType, LogType.DB_Elapsed_Begin, "limisGenedt:" + limsGenedt.Rows.Count.ToString() + "drList:" + drList.Count(), "", "", 0);
                                //添加进mongo
                                GenoWork.GetGeneInfo(null, null, dtMarker, drList, 1, 0, perId, sampId);

                                string updSql = "UPDATE comparedatatask SET  `ex8` = 1 WHERE `id` = " + id + " AND `server` = " + server + ";";
                                dBHelper.execSql(updSql);
                            }
                        }
                        else
                        {
                            //脏数据
                            string updSql = "UPDATE comparedatatask SET  `ex8` = 3 WHERE `id` = " + id + " AND `server` = " + server + ";";
                            dBHelper.execSql(updSql);
                        }
                    }
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            finally
            {
                dBHelper.closeConn();
            }
        }

        /// <summary>
        /// 同步mysql到mongo的家系基因方法定时器
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void GetFamilyGenoInfo_MySQL_Elapsed(Object sender, System.Timers.ElapsedEventArgs e)
        {
            if (FamilySyncServicesStatus_MySQL == STATUS.UnInit) return;
            if (FamilySyncServicesStatus_MySQL != STATUS.Idle)
            {
                //内存数据等同于mongo内已同步数据
                GetRAMCount();
                if (Family_MONGO_AllCount_MySQL == Family_DB_AllCount_MySQL || FamilySyncServicesStatus_MySQL == STATUS.Exiting)
                {
                    FamilySyncServicesStatus_MySQL = STATUS.Idle;
                    return;
                }
                if (FamilySyncServicesStatus_MySQL == STATUS.Initing) return;
            }
            //GetFamilyGeneInfo.Enabled = true;
            FamilySyncServicesStatus_MySQL = STATUS.Initing;
            FamilySyncServiceStatus = STATUS.Exiting;
        }

        /// <summary>
        /// 同步删除掉的mysql和mongo的数据
        /// 普通基因
        /// </summary>
        private static void DeleteGeneAsync_MySQL()
        {
            Interlocked.Increment(ref StrDeleteLock);
            IMongoDatabase mongobase = MongoDBHelper.createMongoConnection(DBConfig.ConnectionString, DBConfig.LDBNAME);
            IDBHelper dBHelper1= new DBHelperMySQL("", Program.Settings.DNALims);
            int pageIndex = 1;
            int pageSize = Program.Settings.Limit;
            try
            {
                ParallelOptions parallelOptions = new ParallelOptions();
                parallelOptions.MaxDegreeOfParallelism = 1;// GeneSyncThreadCount;

                string maxIdSql = "SELECT max(id) as maxId FROM `comparedatatask` where ex8=0 and `status`=1";
                var id = dBHelper1.execScalar(maxIdSql);
                dBHelper1.closeConn();
                if (!id.Equals(DBNull.Value))
                {
                    var count = Convert.ToInt32(id);
                    int page = (count % pageSize == 0 ? count / pageSize : count / pageSize + 1);

                    //Parallel.For(0, page, parallelOptions, p =>
                    //{
                    //    DeleteParaller(p, pageSize);
                    //});
                    for (int p = 0; p < page; p++)
                    {
                        DeleteParaller(p, pageSize);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            finally
            {
                Interlocked.Decrement(ref StrDeleteLock);
            }
        }

        private static void DeleteParaller(int pageIndex,int pageSize)
        {
            IDBHelper dBHelper = new DBHelperMySQL("", Program.Settings.DNALims);
            IMongoDatabase mongobase = MongoDBHelper.createMongoConnection(DBConfig.ConnectionString, DBConfig.LDBNAME);
            string sql = $"SELECT * from comparedatatask where id>={pageIndex * pageSize}  and id <{pageIndex * pageSize + pageSize} and ex8=0 and `status`=1 ";
            var genedt = dBHelper.getDataTable(sql);
            if (genedt != null && genedt.Rows.Count > 0)
            {
                for (int k = 0; k < genedt.Rows.Count; k++)
                {
                    lock (genedt.Rows[k])
                    {
                        var compareId = genedt.Rows[k]["id"].ToString();
                        var gid = genedt.Rows[k]["geneindexid"].ToString();
                        var server = genedt.Rows[k]["server"].ToString();
                        var id = gid + ":" + server;

                        string sqlindex = $"select * from geneindex where idper = (select idper from geneindex where id={gid} and `server`={server})and idsampling<>0 and `server`={server} and deleted=1";
                        var dtIndex = dBHelper.getDataTable(sqlindex);

                        if (dtIndex != null && dtIndex.Rows.Count > 0)
                        {
                            string updSql = $"update comparedatatask set ex8=1 where geneindexid={gid} and id={compareId} and `STATUS`=1";
                            dBHelper.execSql(updSql);

                            var perId = dtIndex.Rows[0]["idper"].ToString();

                            //删除mongo数据
                            var tableName = "";
                            for (int i = 0; i < 3; i++)
                            {
                                switch (i)
                                {
                                    case 0:
                                        tableName = "Gene_";
                                        break;
                                    case 1:
                                        tableName = "XGene_";
                                        break;
                                    case 2:
                                        tableName = "YGene_";
                                        break;
                                }
                                for (short j = 0; j < 10; j++)
                                {
                                    var LDBNAME = tableName + j;
                                    var collection = mongobase.GetCollection<BsonDocument>(LDBNAME);


                                    var geneList = collection
                                        .Find(Builders<BsonDocument>.Filter.Eq("_id", id)).ToList();
                                    if (geneList.Count > 0)
                                    {
                                        switch (i)
                                        {
                                            case 0 when (Program.GeneItems_dict == null || Program.GeneItems_dict.Count <= 0):
                                                continue;
                                            case 0:
                                                Program.GeneItems_dict[j].GenoSamples.Remove(id);
                                                break;
                                            case 1 when (Program.GeneItemsX_dict == null || Program.GeneItemsX_dict.Count <= 0):
                                                continue;
                                            case 1:
                                                Program.GeneItemsX_dict[j].GenoSamples.Remove(id);
                                                break;
                                            case 2 when (Program.GeneItemsY_dict == null || Program.GeneItemsY_dict.Count <= 0):
                                                continue;
                                            case 2:
                                                Program.GeneItemsY_dict[j].GenoSamples.Remove(id);
                                                break;
                                        }
                                        collection.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", id), Builders<BsonDocument>.Update.Set("DELETE_FLAG", 1));
                                        ////logWork.AddLog(DateTime.Now, id, LDBNAME + "删除数据,_id:" + id, LogType.DB_Elapsed_Begin, "0", "1.50", "192.168.31.254", 5001);
                                    }
                                }
                            }

                            //验证是否存在家系关联
                            DeleteFamilyAsync_MySQL(perId + ":" + server);
                        }
                    }
                }
            }
            dBHelper.closeConn();
        }

        /// <summary>
        /// 同步删除掉的mysql和mongo的数据
        /// 家系
        /// </summary>
        private static void DeleteFamilyAsync_MySQL(string perid)
        {
            IMongoDatabase mongobase = MongoDBHelper.createMongoConnection(DBConfig.ConnectionString, DBConfig.LDBNAME);
            string tableName = "Family";
            for (int i = 0; i < 10; i++)
            {
                string LDBCNAME = tableName + "_" + i;
                var collection = mongobase.GetCollection<BsonDocument>(LDBCNAME);
                try
                {
                    var bson = collection.Find(
                    Builders<BsonDocument>.Filter.And(
                        Builders<BsonDocument>.Filter.Eq("PERID", perid),
                        Builders<BsonDocument>.Filter.Eq("DELETE_FLAG", 0)
                        )).ToList();
                    if (bson.Count > 0)
                    {
                        foreach (var item in bson)
                        {
                            string[] info = item.GetValue("PERID").AsString.Split(":");
                            var dicttype = item.GetValue("DICTTYPE").AsInt32;
                            var index = item.GetValue("MONGOINDEX").AsInt32;
                            var guid = item.GetValue("_id").AsString;

                            // -1 范围外 未建立关系
                            //  0 F c
                            //  1 M c
                            //  2 F M
                            for (short j = 0; j < 10; j++)
                            {
                                switch (dicttype)
                                {
                                    case 0:
                                        Program.GenoItems_Family[j].DictFAC.Remove(perid);
                                        break;
                                    case 1:
                                        Program.GenoItems_Family[j].DictMAC.Remove(perid);
                                        break;
                                    case 2:
                                        Program.GenoItems_Family[j].DictFAM.Remove(perid);
                                        break;
                                    default:
                                        break;
                                }
                            }

                            collection.UpdateOne(
                               Builders<BsonDocument>.Filter.And(
                                   Builders<BsonDocument>.Filter.Eq("_id", guid),
                                   Builders<BsonDocument>.Filter.Eq("PERID", perid)),
                               Builders<BsonDocument>.Update.Set("DELETE_FLAG", 1));
                        }
                    }
                }
                catch (Exception)
                {
                }
            }
        }

        private static int GetUpdateTask_MySQL()
        {
            IDBHelper dBHelper = new DBHelperMySQL("", Program.Settings.DNALims);
            string sql = "select count(*) from comparedatatask where ex8 = 0";
            var count= Convert.ToInt32(dBHelper.execScalar(sql));
            dBHelper.closeConn();
            return count;
        }

        #endregion

        #region mongo数据同步内存
        /// <summary>
        /// 同步STR基因定时器
        /// </summary>
        private static readonly System.Timers.Timer GetCurGeneInfo = new System.Timers.Timer();
        /// <summary>
        /// 同步家系基因定时器
        /// </summary>
        private static readonly System.Timers.Timer GetFamilyGeneInfo = new System.Timers.Timer();

        /// <summary>
        /// 常XY基因同步服务状态
        /// </summary>
        public static STATUS StrSyncServiceStatus = STATUS.UnInit;
        /// <summary>
        /// 家系基因同步服务状态
        /// </summary>
        public static STATUS FamilySyncServiceStatus = STATUS.UnInit;

        /// <summary>
        /// 数据库中常XY基因数据总数量
        /// </summary>
        public static long STR_DB_AllCount;
        /// <summary>
        /// 同步到内存中的常XY基因数量
        /// </summary>
        public static long STR_RAM_AllCount;
        public static long MIX_RAM_AllCount;

        /// <summary>
        /// 数据库中家系数据总数量
        /// </summary>
        public static long Family_DB_AllCount;
        /// <summary>
        /// 同步到内存中的家系基因数量
        /// </summary>
        public static long Family_RAM_AllCount;

        /// <summary>
        /// 被删除的基因
        /// </summary>
        public static List<Dictionary<string, string>> DeleteGeneList = new List<Dictionary<string, string>>();
        /// <summary>
        /// 被删除的家系基因
        /// </summary>
        public static List<Dictionary<string, string>> DeleteFamilyList = new List<Dictionary<string, string>>();


        /// <summary>
        /// 同步常xy基因方法的定时器
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void GetCurGeneInfo_Elapsed(Object sender, System.Timers.ElapsedEventArgs e)
        {
            if (StrSyncServiceStatus == STATUS.UnInit) return;
            //验证已删除数据
            //DeleteGeneAsync();
            if (StrSyncServiceStatus != STATUS.Idle)
            {
                GetRAMCount();
                if (STR_RAM_AllCount == STR_DB_AllCount || StrSyncServiceStatus == STATUS.Exiting)
                {
                    StrSyncServiceStatus = STATUS.Idle;
                    return;
                }

                if (StrSyncServiceStatus == STATUS.Initing) return;
            }


            //2019-9-3 根据isodatetime 更新数据
            /*
            if (StrSyncServiceStatus == STATUS.Idle)
            {
                GetDBCount();


                if (STR_DB_AllCount <= STR_RAM_AllCount)
                {
                    return;
                }

            }
            */
            //if (StrSyncServiceStatus == STATUS.Initing) return;
            StrSyncServiceStatus = STATUS.Initing;
            if (STR_RAM_AllCount > STR_DB_AllCount && DeleteGeneList.Count == 0)
            {
                //内存中数据比mongo多，并且没有待删除数据
                List<string> delList = MonitoringController.GetDataNotExistInDB(1);
                for (int i = 0; i < delList.Count; i++)
                {
                    var keyValuePairs = new Dictionary<string, string>();
                    keyValuePairs["GENEID"] = delList[i].Split(":")[0];
                    keyValuePairs["SERVER"] = delList[i].Split(":")[1];
                    lock (DeleteGeneList)
                    {
                        DeleteGeneList.Add(keyValuePairs);
                    }
                }
            }

            try
            {
                for (short i = 0; i < 10; i++)
                {
                    GetGeneSync("Gene", i);
                }
                for (short i = 0; i < 10; i++)
                {
                    GetGeneSync("XGene", i);
                }
                for (short i = 0; i < 10; i++)
                {
                    GetGeneSync("YGene", i);
                }
            }
            catch (Exception exception)
            {
                Console.WriteLine(exception);
            }
            StrSyncServiceStatus = STATUS.Exiting;
        }

        /// <summary>
        /// 同步家系基因方法的定时器
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void GetFamilyGeneInfo_Elapsed(Object sender, System.Timers.ElapsedEventArgs e)
        {
            if (FamilySyncServiceStatus == STATUS.UnInit) return;
            //验证已删除数据
            //DeleteFamilyAsync();
            if (FamilySyncServiceStatus != STATUS.Idle)
            {
                GetRAMCount();
                if (Family_RAM_AllCount == Family_DB_AllCount || FamilySyncServiceStatus == STATUS.Exiting)
                {
                    FamilySyncServiceStatus = STATUS.Idle;
                    return;
                }
                if (FamilySyncServiceStatus == STATUS.Initing) return;
            }

            //2019-9-3 根据isodatetime 更新数据
            /*
            if (FamilySyncServiceStatus == STATUS.Idle)
            {
                GetDBCount();
                if (Family_DB_AllCount <= Family_RAM_AllCount)
                {
                    return;
                }
            }
            */

            FamilySyncServiceStatus = STATUS.Initing;
            if (Family_RAM_AllCount > Family_DB_AllCount && DeleteFamilyList.Count == 0)
            {
                //内存中数据比mongo多，并且没有待删除数据
                List<string> delList = MonitoringController.GetDataNotExistInDB(2);
                for (int i = 0; i < delList.Count; i++)
                {
                    DeleteFamilyAsync_MySQL(delList[i]);
                }
            }

            try
            {
                for (short i = 0; i < 10; i++)
                {
                    GetFamilyGeneAsync("Family", i);
                }
            }
            catch (Exception exception)
            {
                Console.WriteLine(exception);
            }
            FamilySyncServiceStatus = STATUS.Exiting;
        }

        /// <summary>
        /// 同步常xy基因到内存
        /// </summary>
        /// <param name="table"></param>
        /// <param name="index"></param>
        public static void GetGeneSync(string table, short index)
        {
            IMongoDatabase mongobase = MongoDBHelper.createMongoConnection(DBConfig.ConnectionString, DBConfig.LDBNAME);
            IDBHelper dBHelper = new DBHelperMySQL("", Program.Settings.DNALims);
            string LDBCNAME = table + "_" + index;
            var collection = mongobase.GetCollection<BsonDocument>(LDBCNAME);


            Dictionary<string, GenoSample> genos = new Dictionary<string, GenoSample>();

            var maxTime = DateTime.MinValue;
            string[] markerList = null;
            TGenoType GenoType = TGenoType.STR;
            switch (table)
            {
                case "Gene":
                    var time = Program.GeneItems_dict[index].ISODateTime;
                    maxTime = time != DateTime.MinValue ? time.AddHours(-8) : DateTime.MinValue;
                    markerList = Program.GENOMARKER.MARKER.MarkerStandardList;
                    break;
                case "XGene":
                    var xtime = Program.GeneItemsX_dict[index].ISODateTime;
                    maxTime = xtime != DateTime.MinValue ? xtime.AddHours(-8) : DateTime.MinValue;
                    markerList = Program.GENOMARKER.MARKER_X.MarkerStandardList;
                    GenoType = TGenoType.XSTR;
                    break;
                case "YGene":
                    var ytime = Program.GeneItemsY_dict[index].ISODateTime;
                    maxTime = ytime != DateTime.MinValue ? ytime.AddHours(-8) : DateTime.MinValue;
                    markerList = Program.GENOMARKER.MARKER_Y.MarkerStandardList;
                    GenoType = TGenoType.YSTR;
                    break;
            }
            var bson = collection.Find(
                Builders<BsonDocument>.Filter.And(Builders<BsonDocument>.Filter.Gte("ISODATE", maxTime))).ToList();
            
            foreach (var item in bson)
            {
                lock (item)
                {
                    try
                    {
                        //无法验证delete_flag是谁修改的，每次同步获取到都要添加进删除队列
                        string _id = item.GetValue("_id").AsString;
                        if (Convert.ToInt32(item.GetValue("DELETE_FLAG")) == 1)
                        {
                            //    string indexsql = string.Format("select id from comparedatatask where `status`=1 and ex8=0 and geneindexid={0}", _id.Split(":")[0]);
                            //    var compareId = dBHelper.execScalar(indexsql);
                            //    if (compareId!=null && compareId.ToString().Length>0)
                            //    {
                            //        lock (DeleteGeneList)
                            //        {
                            //            Dictionary<string, string> dict = new Dictionary<string, string>();
                            //            dict["GENEID"] = _id.Split(":")[0];
                            //            dict["SERVER"] = _id.Split(":")[1];
                            //            dict["TYPE"] = "0";
                            //            DeleteGeneList.Add(dict);
                            //        }
                            //    }
                            //    else
                            //    {
                            //        collection.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", _id), Builders<BsonDocument>.Update.Set("DELETE_FLAG", 0));
                            //    }
                            continue;
                        }
                        GenoItems genoItems = new GenoItems();
                        genoItems.ISODateTime = Convert.ToDateTime(item.GetValue("ISODATE").AsBsonDateTime);
                        genoItems.MongoIndex = Convert.ToInt16(item.GetValue("MONGOINDEX"));
                        genoItems.GenoStatus = STATUS.UnInit;

                        GenoSample geno = new GenoSample();
                        geno.GenoType = GenoType;
                        geno.isMix = item.GetValue("MIX").AsInt32;
                        geno.PerId = item.GetValue("IDPER").AsString;
                        geno.SampleId = item.GetValue("IDSAMPLINGLIMS").AsString;
                        geno.isMix = item.GetValue("MIX").AsInt32;
                        geno.Category = item.GetValue("CATEGORY").AsInt32;

                        BsonDateTime b = item.GetValue("ISODATE").AsBsonDateTime;
                        DateTime dateTime = Convert.ToDateTime(b).AddHours(8);

                        if (genoItems.ISODateTime < dateTime)
                        {
                            genoItems.ISODateTime = dateTime;
                        }

                        foreach (BsonElement be in item["GENO"].AsBsonDocument)
                        {
                            if (!be.Value.IsBsonNull)
                            {
                                LociSample ml = new LociSample(be.Name, be.Value.AsString, markerList);
                                geno.locus[ml.imarker] = ml;
                            }
                        }

                        genos[string.Intern(_id)] = geno;
                        genoItems.GenoSamples = genos;
                        //GenoMix维护
                        switch (geno.isMix)
                        {
                            case 0:
                                switch (table)
                                {
                                    case "Gene":
                                        Program.GeneItems_dict[index].GenoSamples[_id] = geno;
                                        Program.GeneItems_dict[index].ISODateTime = genoItems.ISODateTime;
                                        Program.GeneItems_dict[index].GenoStatus = STATUS.Idle;
                                        break;
                                    case "XGene":
                                        Program.GeneItemsX_dict[index].GenoSamples[_id] = geno;
                                        Program.GeneItemsX_dict[index].ISODateTime = genoItems.ISODateTime;
                                        Program.GeneItemsX_dict[index].GenoStatus = STATUS.Idle;
                                        break;
                                    case "YGene":
                                        Program.GeneItemsY_dict[index].GenoSamples[_id] = geno;
                                        Program.GeneItemsY_dict[index].ISODateTime = genoItems.ISODateTime;
                                        Program.GeneItemsY_dict[index].GenoStatus = STATUS.Idle;
                                        break;
                                }
                                break;
                            case 1:
                                if (Program.GeneItemsMix_dict==null)
                                {
                                    goto case 0;
                                }
                                else
                                {
                                    var mongoIndex = item.GetValue("MONGOINDEX").AsInt32;
                                    if (Program.GeneItemsMix_dict[(short)mongoIndex].GenoSamples.ContainsKey(string.Intern(_id)))
                                    {
                                        Program.GeneItemsMix_dict[(short)mongoIndex].GenoSamples[string.Intern(_id)] = geno;
                                    }
                                }
                                break;
                        }

                        //GenoAlls维护
                        GenoSampleModel sampleModel = null;
                        if (Program.GenoAlls.ContainsKey(_id))
                        {
                            sampleModel = Program.GenoAlls[_id];
                        }
                        if (sampleModel == null)
                        {
                            sampleModel = new GenoSampleModel();
                            sampleModel.Id = _id;
                            sampleModel.PerId = geno.PerId;
                        }

                        if (geno.GenoType == TGenoType.STR)
                        {
                            sampleModel.Geno = geno;
                        }
                        else if (geno.GenoType == TGenoType.YSTR)
                        {
                            sampleModel.Ygeno = geno;
                        }
                        else if (geno.GenoType == TGenoType.XSTR)
                        {
                            sampleModel.Xgeno = geno;
                        }

                        lock (Program.GenoAlls)
                        {
                            Program.GenoAlls[_id] = sampleModel;
                        }

                        
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex);
                    }
                }
            }
        }

        /// <summary>
        /// 同步家系基因到内存
        /// </summary>
        /// <param name="table"></param>
        /// <param name="key"></param>
        private static void GetFamilyGeneAsync(string table, short key)
        {
            IMongoDatabase mongobase = MongoDBHelper.createMongoConnection(DBConfig.ConnectionString, DBConfig.LDBNAME);
            string LDBCNAME = table + "_" + key;
            var collection = mongobase.GetCollection<BsonDocument>(LDBCNAME);

            //TODO:测试不减8小时
            //var maxTime = DateTime.MinValue;
            //var time = GenoItems_Family[key].ISODateTime;
            //maxTime = time != DateTime.MinValue ? time.AddHours(-8) : DateTime.MinValue;
            var maxTime = DateTime.MinValue;
            var time = Program.GenoItems_Family[key].ISODateTime;
            maxTime = time != DateTime.MinValue ? time.AddHours(-8) : DateTime.MinValue;
            var bsonList = collection
                .Find(Builders<BsonDocument>.Filter.And(Builders<BsonDocument>.Filter.Gte("ISODATE", maxTime))).ToList();
            Program.wrls.EnterReadLock();
            foreach (var item in bsonList)
            {
                try
                {
                    string guid = item.GetValue("_id").AsString;
                    string id = item.GetValue("PERID").AsString;
                    if (Convert.ToInt32(item.GetValue("DELETE_FLAG")) == 1)
                    {
                        //    lock (DeleteFamilyList)
                        //    {
                        //        Dictionary<string, string> dict = new Dictionary<string, string>();
                        //        dict["_id"] = guid;
                        //        dict["GENEID"] = id.Split(":")[0];
                        //        dict["SERVER"] = id.Split(":")[1];
                        //        dict["TYPE"] = item.GetValue("DICTTYPE").AsString;
                        //        dict["MONGOINDEX"] = item.GetValue("MONGOINDEX").AsString;
                        //        DeleteFamilyList.Add(dict);
                        //    }
                        continue;
                    }

                    List<GenoPer> genoPerls = new List<GenoPer>();
                    BsonDocument bsonRelF;
                    BsonDocument bsonRelM;
                    BsonDocument bsonRelC;

                    BsonValue dictType = item.GetValue("DICTTYPE");
                    short mongoIndex = Convert.ToInt16(item.GetValue("MONGOINDEX"));
                    DateTime isoDateTime = Convert.ToDateTime(item.GetValue("ISODATE").AsBsonDateTime);
                    //TODO：稍后提取通用方法
                    GenoPer gp = new GenoPer();
                    switch (dictType.AsInt32)
                    {
                        case 0:
                            gp.GenoFather = GenoWork.SetGenoRel(item["F"].AsBsonDocument);
                            gp.GenoChild = GenoWork.SetGenoRel(item["C"].AsBsonDocument);
                            genoPerls.Add(gp);

                            Program.GenoItems_Family[mongoIndex].DictFAC[id] = genoPerls;
                            Program.GenoItems_Family[mongoIndex].ISODateTime = isoDateTime;
                            break;
                        case 1:
                            gp.GenoMother = GenoWork.SetGenoRel(item["M"].AsBsonDocument);
                            gp.GenoChild = GenoWork.SetGenoRel(item["C"].AsBsonDocument);
                            genoPerls.Add(gp);

                            Program.GenoItems_Family[mongoIndex].DictMAC[id] = genoPerls;
                            Program.GenoItems_Family[mongoIndex].ISODateTime = isoDateTime;
                            break;
                        case 2:
                            gp.GenoFather = GenoWork.SetGenoRel(item["F"].AsBsonDocument);
                            gp.GenoMother = GenoWork.SetGenoRel(item["M"].AsBsonDocument);
                            genoPerls.Add(gp);

                            Program.GenoItems_Family[mongoIndex].DictFAM[id] = genoPerls;
                            Program.GenoItems_Family[mongoIndex].ISODateTime = isoDateTime;
                            break;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
            }
            Program.wrls.ExitReadLock();
        }

        /// <summary>
        /// 同步删除掉的内存和mongo的数据
        /// 普通基因
        /// </summary>
        private static void DeleteGeneAsync()
        {
            IMongoDatabase mongobase = MongoDBHelper.createMongoConnection(DBConfig.ConnectionString, DBConfig.LDBNAME);
            Program.wrls.EnterReadLock();
            while (DeleteGeneList.Count > 0 && DeleteGeneList != null)
            {
                Dictionary<string, string> dictionary;
                lock (DeleteGeneList)
                {
                    dictionary = DeleteGeneList[0];
                }
                string gid = dictionary["GENEID"];
                string server = dictionary["SERVER"];

                var id = gid + ":" + server;

                var tableName = "";
                for (int i = 0; i < 3; i++)
                {
                    switch (i)
                    {
                        case 0:
                            tableName = "Gene_";
                            break;
                        case 1:
                            tableName = "XGene_";
                            break;
                        case 2:
                            tableName = "YGene_";
                            break;
                    }
                    for (short j = 0; j < 10; j++)
                    {
                        var LDBNAME = tableName + j;
                        var collection = mongobase.GetCollection<BsonDocument>(LDBNAME);


                        var geneList = collection
                            .Find(Builders<BsonDocument>.Filter.Eq("_id", id)).ToList();
                        if (geneList.Count > 0)
                        {
                            switch (i)
                            {
                                case 0 when (Program.GeneItems_dict == null || Program.GeneItems_dict.Count <= 0):
                                    continue;
                                case 0:
                                    Program.GeneItems_dict[j].GenoSamples.Remove(id);
                                    DeleteGeneList.Remove(dictionary);
                                    break;
                                case 1 when (Program.GeneItemsX_dict == null || Program.GeneItemsX_dict.Count <= 0):
                                    continue;
                                case 1:
                                    Program.GeneItemsX_dict[j].GenoSamples.Remove(id);
                                    DeleteGeneList.Remove(dictionary);
                                    break;
                                case 2 when (Program.GeneItemsY_dict == null || Program.GeneItemsY_dict.Count <= 0):
                                    continue;
                                case 2:
                                    Program.GeneItemsY_dict[j].GenoSamples.Remove(id);
                                    DeleteGeneList.Remove(dictionary);
                                    break;
                            }
                            collection.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", id), Builders<BsonDocument>.Update.Set("DELETE_FLAG", 1));
                            ////logWork.AddLog(DateTime.Now, id, LDBNAME + "删除数据,_id:" + id, LogType.DB_Elapsed_Begin, "0", "1.50", "192.168.31.254", 5001);
                        }
                    }
                }
            }
            Program.wrls.ExitReadLock();
            GetRAMCount();
        }

        /// <summary>
        /// 同步删除掉的内存和mongo的数据
        /// 家系
        /// </summary>
        private static void DeleteFamilyAsync()
        {
            IMongoDatabase mongobase = MongoDBHelper.createMongoConnection(DBConfig.ConnectionString, DBConfig.LDBNAME);
            Program.wrls.EnterReadLock();
            while (DeleteFamilyList.Count > 0 && DeleteFamilyList != null)
            {
                Dictionary<string, string> dictionary;
                lock (DeleteFamilyList)
                {
                    dictionary = DeleteFamilyList[0];
                }
                string guid = dictionary["_id"];
                string server = dictionary["SERVER"];
                string perid = dictionary["GENEID"];
                short mongoIndex = Convert.ToInt16(dictionary["MONGOINDEX"]);

                var id = perid + ":" + server;

                var tableName = "Family_";
                var LDBNAME = tableName + mongoIndex;
                var collection = mongobase.GetCollection<BsonDocument>(LDBNAME);
                var geneList = collection.Find(
                        Builders<BsonDocument>.Filter.And(
                            Builders<BsonDocument>.Filter.Eq("_id", guid),
                            Builders<BsonDocument>.Filter.Eq("DELETE_FLAG", 0),
                            Builders<BsonDocument>.Filter.Eq("PERID", id)
                            )).ToList();
                if (geneList.Count > 0)
                {
                    //设置关系
                    var gp = new GenoPer();
                    foreach (var mg in geneList)
                    {
                        var bsElements = mg.Elements.Skip(1).Take(mg.Elements.Count() - 1).ToList();
                        foreach (var relation in bsElements)
                        {
                            if (relation.Name == "F")
                            {
                                gp.GenoFather = new GenoRel();
                            }
                            if (relation.Name == "M")
                            {
                                gp.GenoMother = new GenoRel();
                            }
                            if (relation.Name == "C")
                            {
                                gp.GenoChild = new GenoRel();
                            }
                        }
                    }
                    //判断关系
                    if (gp.GenoFather != null && gp.GenoChild != null && gp.GenoMother == null)
                    {
                        Program.GenoItems_Family[mongoIndex].DictFAC.Remove(id);
                        DeleteFamilyList.Remove(dictionary);
                    }
                    else if (gp.GenoMother != null && gp.GenoChild != null && gp.GenoFather == null)
                    {
                        Program.GenoItems_Family[mongoIndex].DictMAC.Remove(id);
                        DeleteFamilyList.Remove(dictionary);
                    }
                    else if (gp.GenoFather != null && gp.GenoMother != null && gp.GenoChild == null)
                    {
                        Program.GenoItems_Family[mongoIndex].DictFAM.Remove(id);
                        DeleteFamilyList.Remove(dictionary);
                    }
                    collection.UpdateOne(
                        Builders<BsonDocument>.Filter.And(
                            Builders<BsonDocument>.Filter.Eq("_id", guid),
                            Builders<BsonDocument>.Filter.Eq("PERID", id)),
                        Builders<BsonDocument>.Update.Set("DELETE_FLAG", 1));
                    //logWork.AddLog(DateTime.Now, id, LDBNAME + "删除数据,perid:" + id, LogType.DB_Elapsed_Begin, "0", "1.50", "localhost", 5001);
                }
            }
            Program.wrls.ExitReadLock();
            GetRAMCount();
        }

        /// <summary>
        /// 获取内存中的基因数量
        /// </summary>
        /// <returns></returns>
        public static List<string> GetRAMCount()
        {
            List<string> info = new List<string>();

            int strCount = 0;
            int xStrCount = 0;
            int yStrCount = 0;

            for (short i = 0; i < Program.GeneItems_dict.Count; i++)
            {
                var count = Program.GeneItems_dict[i].GenoSamples.Count;
                strCount += count;
                info.Add(string.Format("常STR_{0} : {1}", i, count));
            }
            for (short i = 0; i < Program.GeneItemsX_dict.Count; i++)
            {
                var count = Program.GeneItemsX_dict[i].GenoSamples.Count;
                xStrCount += count;
                info.Add(string.Format("XSTR_{0} : {1}", i, count));
            }
            for (short i = 0; i < Program.GeneItemsY_dict.Count; i++)
            {
                var count = Program.GeneItemsY_dict[i].GenoSamples.Count;
                yStrCount += count;
                info.Add(string.Format("YSTR_{0} : {1}", i, count));
            }

            int mixCount = 0;
            for (short i = 0; i < Program.GeneItemsMix_dict.Count; i++)
            {
                var count = Program.GeneItemsMix_dict[i].GenoSamples.Count;
                mixCount += count;
                info.Add(string.Format("Mix_{0} : {1}", i, count));
            }
            MIX_RAM_AllCount = mixCount;
            STR_RAM_AllCount = strCount + xStrCount + yStrCount+mixCount;
            STR_MONGO_AllCount_MySQL = STR_RAM_AllCount;
            //family
            int FAC = 0;
            int FAM = 0;
            int MAC = 0;

            foreach (var key in Program.GenoItems_Family.Keys)
            {
                FAC += Program.GenoItems_Family[key].DictFAC.Keys.Count;
                MAC += Program.GenoItems_Family[key].DictMAC.Keys.Count;
                FAM += Program.GenoItems_Family[key].DictFAM.Keys.Count;
            }

            info.Add(string.Format("Familyt_FAM:{0}", FAM));
            info.Add(string.Format("Familyt_FAC:{0}", FAC));
            info.Add(string.Format("Familyt_MAC:{0}", MAC));

            Family_RAM_AllCount = FAC + FAM + MAC;
            Family_MONGO_AllCount_MySQL = Family_RAM_AllCount;
            
            int GenoDeleteCount = DeleteGeneList.Count;
            int FamilyDeleteCount = DeleteFamilyList.Count;

            //info.Add($"Gene待删除数据:{GenoDeleteCount}");
            //info.Add($"家系待删除数据:{FamilyDeleteCount}");
            return info;
        }

        /// <summary>
        /// 获取mongodb数据库中数据的数量
        /// </summary>
        public static void GetDBCount_MongoDB()
        {
            IMongoDatabase mongobase = MongoDBHelper.createMongoConnection(DBConfig.ConnectionString, DBConfig.LDBNAME);
            long strAllCount = 0;
            long familyAllCount = 0;
            var filter = Builders<BsonDocument>.Filter.Eq("DELETE_FLAG", 0);
            FieldDefinition<BsonDocument, string> fieldID = "_id";
            for (int i = 0; i < 10; i++)
            {
                string LDBCNAME = "Gene" + "_" + i;
                var collection = mongobase.GetCollection<BsonDocument>(LDBCNAME);
                strAllCount += collection.Distinct(fieldID, filter).ToList().Count();
            }
            for (int i = 0; i < 10; i++)
            {
                string LDBCNAME = "XGene" + "_" + i;
                var collection = mongobase.GetCollection<BsonDocument>(LDBCNAME);
                strAllCount += collection.Distinct(fieldID, filter).ToList().Count();
            }
            for (int i = 0; i < 10; i++)
            {
                string LDBCNAME = "YGene" + "_" + i;
                var collection = mongobase.GetCollection<BsonDocument>(LDBCNAME);
                strAllCount += collection.Distinct(fieldID, filter).ToList().Count();
            }
            STR_DB_AllCount = strAllCount;

            FieldDefinition<BsonDocument, string> fieldPERID = "PERID";
            for (short i = 0; i < 10; i++)
            {
                string LDBCNAME = "Family" + "_" + i;
                var collection = mongobase.GetCollection<BsonDocument>(LDBCNAME);
                familyAllCount += collection.Distinct(fieldPERID, filter).ToList().Count();
            }

            Family_DB_AllCount = familyAllCount;
        }

        #endregion
    }
}
