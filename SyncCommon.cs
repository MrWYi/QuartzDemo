using CacheModel.Model;
using CacheModel.Model.ModelFamliy;
using Dal;
using GenoType2MDB.Match;
using Model.NetCore;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoLib;
using RX.DNA.Match.MatchUnit.Genes;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using WebApplication1;

namespace MatchUnit.Common.SyncDatas
{
    public static class SyncCommon
    {
        #region 数据库同步数据库的方法

        public static void DBSyncManage()
        {
            if (SyncManage.sync_status_db_str == STATUS.Initing) return;
            SyncManage.sync_status_db_str = STATUS.Initing;

            //验证位点
            if (SyncManage.Marker == null || SyncManage.Marker.Rows.Count <= 0) SyncManage.LoadMarker();
            IDBHelper dBHelper = new DBHelperMySQL("", Program.Settings.DNALims);

            try
            {
                //分页同步
                //状态，默认0 插入，1 删除，2 更新 
                string maxIdSql = "SELECT max(id) FROM `comparedatatask` where ex8=0  and `type`=0 order by updatetime desc,`level`";
                var maxId = dBHelper.execScalar(maxIdSql);
                if (maxId.Equals(DBNull.Value))
                {
                    SyncManage.sync_status_db_str = STATUS.Exiting;
                    return;
                }

                string minIdSql = "select min(id) from comparedatatask where ex8=0  and `type`=0 order by updatetime desc,`level`";
                var minId = dBHelper.execScalar(minIdSql);
                if (minId.Equals(DBNull.Value))
                {
                    minId = 0;
                }
               
                DBSync(Convert.ToInt32(minId), Convert.ToInt32(maxId));
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            finally
            {
                dBHelper.closeConn();
                SyncManage.sync_status_db_str = STATUS.Exiting;
            }
        }

        private static void DBSync(int minId, int maxId)
        {
            IDBHelper dBHelper = new DBHelperMySQL("", Program.Settings.DNALims);
            try
            {
                string sql = $"SELECT * from comparedatatask where id BETWEEN {minId} and {maxId}  and ex8=0  and `type`=0 order by updatetime desc,`level`";
                var dtGene = dBHelper.getDataTable(sql);
                if (dtGene != null && dtGene.Rows.Count > 0)
                {
                    for (int i = 0; i < dtGene.Rows.Count; i++)
                    {
                        var status = Convert.ToInt32(dtGene.Rows[i]["status"]);
                        var id = dtGene.Rows[i]["id"].ToString();
                        var geneIndexid = dtGene.Rows[i]["geneindexid"].ToString();
                        var server = dtGene.Rows[i]["server"].ToString();

                        if (status == 1)
                        {
                            Console.WriteLine($"接收到删除数据 compare:{id}  {DateTime.Now}");
                            DeleteGeno(dBHelper,id, geneIndexid, server);
                        }
                        else if (status == 0 || status == 2)
                        {
                            Console.WriteLine($"接收到新增数据 compare:{id}  {DateTime.Now}");
                            AddGeno(dBHelper, id, geneIndexid, server);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                dBHelper.closeConn();
            }
        }

        private static void AddGeno(IDBHelper dBHelper, string id, string geneIndexid, string server)
        {

            string sqlindex = @"select g.id,g.server,g.idsampling,g.idper  from geneindex g,sampling s,per p,commitment c  
                                    where g.idsampling<> 0 and g.idper<> 0 and s.perid<> 0 and s.commitid<> 0 and p.category<>98 
                                    and g.deleted = 0 and p.deleted = 0 and c.deleted = 0 and s.deleted = 0 
                                    and c.`server`= g.`server` and p.`server`= g.`server`and s.`server`= g.`server` 
                                    and s.perid = p.id and s.id = g.idsampling and c.id = s.commitid and g.id='" + geneIndexid + "' and g.`server`='" + server + "'";
            var dtIndex = dBHelper.getDataTable(sqlindex);
            if (dtIndex.Rows.Count > 0)
            {
                //log.Log($"获取基因信息成功 compareId:{id} server:{server} 开始同步");
                for (int k = 0; k < dtIndex.Rows.Count; k++)
                {
                    var sampId = dtIndex.Rows[k]["idsampling"].ToString();
                    var perId = dtIndex.Rows[k]["idper"].ToString();

                    string sqlType = "select * from genotype where id =" + geneIndexid + " and `server`=" + server;
                    var limsGenedt = dBHelper.getDataTable(sqlType);
                    var drList = limsGenedt.Select("id=" + geneIndexid + " and server=" + server);

                    //添加进mongo
                    GenoWork.GetGeneInfo(null, null, SyncManage.Marker, drList, 1, 0, perId, sampId);

                    string updSql = "UPDATE comparedatatask SET  `ex8` = 1 WHERE `id` = " + id + " AND `server` = " + server + ";";
                    dBHelper.execSql(updSql);
                    Console.WriteLine($"{id} 同步成功");
                }
            }
            else
            {
                Console.WriteLine($"{id} 为脏数据");
                //脏数据
                string updSql = "UPDATE comparedatatask SET  `ex8` = 3 WHERE `id` = " + id + " AND `server` = " + server + ";";
                dBHelper.execSql(updSql);
            }
        }

        private static void DeleteGeno(IDBHelper dBHelper,string compareId, string gid, string server)
        {
            IMongoDatabase mongobase = MongoDBHelper.createMongoConnection(DBConfig.ConnectionString, DBConfig.LDBNAME);
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
                        }
                    }
                }
                //验证是否存在家系关联
                DeleteFamily(perId + ":" + server);
            }
        }

        private static void DeleteFamily(string perid)
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
                catch (Exception e)
                {
                    throw e;
                }
            }
        }
        #endregion

        #region 数据库同步内存的方法
        public static void CacheSyncManage_Str()
        {
            if (SyncManage.sync_status_cache_str == STATUS.Initing) return;
            SyncManage.sync_status_cache_str = STATUS.Initing;
            //验证位点
            if (SyncManage.Marker == null || SyncManage.Marker.Rows.Count <= 0) SyncManage.LoadMarker();
            try
            {
                for (short i = 0; i < 10; i++)
                {
                    CacheSync_Str("Gene", i);
                }
                for (short i = 0; i < 10; i++)
                {
                    CacheSync_Str("XGene", i);
                }
                for (short i = 0; i < 10; i++)
                {
                    CacheSync_Str("YGene", i);
                }
            }
            catch (Exception exception)
            {
                Console.WriteLine(exception);
            }
            SyncManage.sync_status_cache_str = STATUS.Exiting;
        }

        private static void CacheSync_Str(string table, short index)
        {
            IMongoDatabase mongobase = MongoDBHelper.createMongoConnection(DBConfig.ConnectionString, DBConfig.LDBNAME);
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
                Builders<BsonDocument>.Filter.And(Builders<BsonDocument>.Filter.Gt("ISODATE", maxTime))).ToList();

            foreach (var item in bson)
            {
                lock (item)
                {
                    try
                    {
                        //无法验证delete_flag是谁修改的，每次同步获取到都要添加进删除队列
                        string _id = item.GetValue("_id").AsString;
                        Console.WriteLine(_id);
                        if (Convert.ToInt32(item.GetValue("DELETE_FLAG")) == 1)
                        {
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
                                        if (Program.GeneItems_dict[index].ISODateTime < genoItems.ISODateTime)
                                        {
                                            Program.GeneItems_dict[index].ISODateTime = genoItems.ISODateTime;
                                        }
                                        Program.GeneItems_dict[index].GenoStatus = STATUS.Idle;

                                        break;
                                    case "XGene":
                                        Program.GeneItemsX_dict[index].GenoSamples[_id] = geno;
                                        if (Program.GeneItemsX_dict[index].ISODateTime < genoItems.ISODateTime)
                                        {
                                            Program.GeneItemsX_dict[index].ISODateTime = genoItems.ISODateTime;
                                        }
                                        Program.GeneItemsX_dict[index].ISODateTime = genoItems.ISODateTime;
                                        Program.GeneItemsX_dict[index].GenoStatus = STATUS.Idle;
                                        break;
                                    case "YGene":
                                        Program.GeneItemsY_dict[index].GenoSamples[_id] = geno;
                                        if (Program.GeneItemsY_dict[index].ISODateTime < genoItems.ISODateTime)
                                        {
                                            Program.GeneItemsY_dict[index].ISODateTime = genoItems.ISODateTime;
                                        }
                                        Program.GeneItemsY_dict[index].ISODateTime = genoItems.ISODateTime;
                                        Program.GeneItemsY_dict[index].GenoStatus = STATUS.Idle;
                                        break;
                                }
                                break;
                            case 1:
                                if (Program.GeneItemsMix_dict == null)
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

        public static void CacheSyncManage_Family()
        {
            if (SyncManage.sync_status_cache_family == STATUS.Initing) return;
            SyncManage.sync_status_cache_family = STATUS.Initing;
            try
            {
                for (short i = 0; i < 10; i++)
                {
                    CacheSync_Family("Family", i);
                }
            }
            catch (Exception exception)
            {
                Console.WriteLine(exception);
            }
            SyncManage.sync_status_cache_family = STATUS.Exiting;
        }

        private static void CacheSync_Family(string table, short key)
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
        #endregion
    }
}
