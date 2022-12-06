//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.cestc.bigdataclient.sqlsubmit;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cestc.bigdataclient.sqlsubmit.bean.vo.SubmitVO;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.sql.parser.ddl.SqlCreateCatalog;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlUseCatalog;
import org.apache.flink.sql.parser.ddl.SqlUseDatabase;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.sql.parser.dql.SqlShowCatalogs;
import org.apache.flink.sql.parser.dql.SqlShowDatabases;
import org.apache.flink.sql.parser.dql.SqlShowTables;
import org.apache.flink.sql.parser.dql.SqlShowViews;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class SqlSubmit {
    private static final Logger logger = LogManager.getLogger(SqlSubmit.class);
    private String sql;
    private String jobName;
    private StreamTableEnvironment tEnv;

    public static void main(String[] args) {
        //String[] pare = new String[]{jsonStr()};
        try {
            SqlSubmit submit = new SqlSubmit(args);
            submit.run();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage(),e);
        } finally {
        }
    }

    public SqlSubmit(String[] args) {
        String submitStr = null;

        try {
            submitStr = URLDecoder.decode(args[0], StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException var4) {
            logger.error("解析参数报错:\n", var4);
        }

        SubmitVO submitVO = (SubmitVO)JSON.parseObject(submitStr, SubmitVO.class);
        this.sql = submitVO.getSql();
        this.jobName = submitVO.getJobId();
    }

    private void run() throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        this.tEnv = StreamTableEnvironment.create(env, settings);
        this.tEnv.getConfig().getConfiguration().setString("pipeline.name", this.jobName);
        SqlParser parser = SqlParser.create(this.sql, SqlParser.configBuilder().setParserFactory(FlinkSqlParserImpl.FACTORY).setQuoting(Quoting.BACK_TICK).setUnquotedCasing(Casing.TO_LOWER).setQuotedCasing(Casing.UNCHANGED).setConformance(FlinkSqlConformance.DEFAULT).build());

        List sqlNodeList;
        try {
            sqlNodeList = parser.parseStmtList().getList();
        } catch (SqlParseException var9) {
            logger.error("解析SQL出错:\n{}\n" + this.sql, var9);
            return;
        }

        StatementSet statementSet = this.tEnv.createStatementSet();
        if (!CollectionUtils.isEmpty(sqlNodeList)) {
            Iterator var6 = sqlNodeList.iterator();

            while(true) {
                while(var6.hasNext()) {
                    SqlNode sqlNode = (SqlNode)var6.next();
                    String sql = sqlNode.toString();
                    logger.info("拆分后的SQL为: \n{}", sql);
                    if (!(sqlNode instanceof SqlCreateCatalog) && !(sqlNode instanceof SqlCreateTable) && !(sqlNode instanceof SqlShowCatalogs) && !(sqlNode instanceof SqlShowDatabases) && !(sqlNode instanceof SqlShowTables) && !(sqlNode instanceof SqlShowViews) && !(sqlNode instanceof SqlUseCatalog) && !(sqlNode instanceof SqlUseDatabase)) {
                        if (sqlNode instanceof RichSqlInsert) {
                            statementSet.addInsertSql(sql);
                            logger.info("SQL: \n{}\n写入的目标表为: {}\n", sqlNode, ((RichSqlInsert)sqlNode).getTargetTableID());
                        } else {
                           /* Table table = this.tEnv.sqlQuery(sql);
                            this.tEnv.toDataStream(table).print("kafka data");
                            env.execute("flink_running");*/
                            logger.warn("不支持的SQL语句, 跳过处理: \n{}", sql);
                        }
                    } else {
                        this.tEnv.executeSql(sql);
                    }
                }

                TableResult execute = statementSet.execute();
                execute.getJobClient().get().getJobExecutionResult().get();
                return;
            }
        }

    }

    private static String jsonStr() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("jobId","flink-sql-test");
        jsonObject.put("taskMode","1");
        String sourceSql = "CREATE TABLE KafkaTable ( "
                +"username STRING,"
                +"requrl STRING,"
                +"reqtime BIGINT,"
                +"dealtime TIMESTAMP(3) METADATA FROM 'timestamp'"
                +") WITH ("
                +"'connector' = 'kafka',"
                +"'topic' = 'test_topic',"
                +"'properties.bootstrap.servers' = '192.168.226.110:9092',"
                +"'properties.group.id' = 'flink-kafka',"
                +"'scan.startup.mode' = 'latest-offset',"
                +"'format' = 'json'"
                +")";
        String sinkSql = "CREATE TABLE UserEvent(" +
                "username STRING," +
                "requrl STRING," +
                "reqtime BIGINT," +
                "dealtime TIMESTAMP(3)"+
                ") WITH(" +
                "'connector.type'='jdbc'," +
                "'connector.url'='jdbc:mysql://192.168.226.110:9511/flink_test?characterEncoding=UTF-8&useUnicode=true&useSSL=false&tinyInt1isBit=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai'," +
                "'connector.table'='userevent'," +
                "'connector.username'='root'," +
                "'connector.password'='mysql123%^PASS'," +
                "'connector.write.flush.max-rows'='1'" +
                ")";
        String insertSql="insert into UserEvent(username,requrl,reqtime,dealtime) select userName,reqUrl,reqTime,dealTime from KafkaTable";
        //String insertSql="select * from KafkaTable";
        jsonObject.put("sql",sourceSql+";"+sinkSql+";"+insertSql);
        String jsonStr =  jsonObject.toJSONString();
        System.out.println(jsonStr);
        try {
            return URLEncoder.encode(jsonStr,StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return "";
        }
    }
}
