import pymysql
import argparse
import math
import re
import os


def split_table_filter(table_name, is_regix, regix_str):
    if is_regix:
        if regix_str == '':
            res_table_name = table_name
            numberEndMach = re.match(r'.*(\d)$', table_name)
            if numberEndMach:
                # print(numberEndMach.group())
                matchObj = re.match(r'[A-Za-z_]+(?=_[0-9]+)', table_name)
                # firstObj = re.match()
                # matchObj = re.match(r'[A-Za-z_]+_[0-9]+', line)
                if matchObj:
                    res_table_name = matchObj.group()
                # print("matchObj.group(1) : ", matchObj.group(1))
            # if(res_table_name == 'eventlogs'):
            #     print(res_table_name)
            return res_table_name
        else:
            res_table_name = table_name
            matchObj = re.match(regix_str, table_name)
            if matchObj:
                res_table_name = matchObj.group()
            return res_table_name
    else:
        return table_name


def column_map(mysql_type):
    lower_type = str.lower(mysql_type)
    if str.__contains__(lower_type, 'char'):
        if str.__contains__(lower_type, '('):
            type_split = lower_type.split('(')
            type = type_split[0]
            type_info = type_split[1]
            size = int(type_info.split(')')[0])
            if (size * 3) > 65533:
                return 'string'
            else:
                return f'varchar({size * 3})'
        else:
            return lower_type
    elif lower_type == 'enum':
        return 'varchar(300)'
    elif lower_type == 'time':
        return 'varchar(100)'
    elif str.__contains__(lower_type, 'text') or str.__contains__(lower_type, 'blob') or str.__contains__(lower_type,
                                                                                                          'binary') or str.__contains__(
        lower_type, 'json'):
        return 'string'
    elif str.__contains__(lower_type, 'time') or str.__contains__(lower_type, 'date') or str.__contains__(lower_type,
                                                                                                          'year'):
        return 'datetime'
    elif str.__contains__(lower_type, 'unsigned'):
        if str.__contains__(lower_type, 'bigint'):
            return 'largeint'
        elif str.__contains__(lower_type, 'decimal'):
            return lower_type.split(' ')[0]
        else:
            return 'bigint'
    elif str.__contains__(lower_type, 'float'):
        return str.replace(lower_type, 'float', 'decimal')
    elif str.__contains__(lower_type, 'double'):
        return str.replace(lower_type, 'double', 'decimal')
    elif str.__contains__(lower_type, 'bit(1)'):
        return 'boolean'
    elif str.__contains__(lower_type, 'bit'):
        return 'varchar(300)'
    else:
        return mysql_type



def get_kafka_sql(datasource, source_db, source_table, comment, topic, pulsar_sql_str, sink_db,
                   sink_table_prefix, columns_with_enter, select_columns_with_enter, detail_flag,pri_key):
    detail_flag = True
    regex_str = ''
    regex_note = ''
    split_table_source = source_table
    like_options = ''
    regex_flag = str.__contains__(comment, '分表结构来源')
    if regex_flag:
        regex_str = "'db-json.tableNameRegrex' = '{}+(?=_[0-9]+)',"
        regex_note = '分表,请注意分表正则！！！！'
        if detail_flag:
            print(f'{sink_db}_{sink_table_prefix}_{source_table}   分表结构来源：{split_table_source}')
            print('>>>>>>    ', comment)
        split_table_source = str.replace(comment.split('分表结构来源')[1], ')', '')
        # print(f'{sink_db}_{sink_table_prefix}_{source_table}   分表结构来源：{split_table_source}')

    pulsar_sql_detail = pulsar_sql_str + ','

    source_catalog_str = ''
    if not detail_flag:
        like_options = f'''like source_catalog.{source_db}.{split_table_source}
(EXCLUDING OPTIONS)'''
        pulsar_sql_detail = ''
        source_catalog_str = f'''create catalog source_catalog with(
    'type' = 'ztn',
    'datasource' = '{datasource}',
    'env' = 'prod'
);'''

    kafka_sql_res = f'''
/*           {regex_note}
***********  {source_db}.{source_table}  ***************************
******作业名: kafka_to_31sr__{sink_db}__{sink_table_prefix}_{source_table}  *********
*/
-- ADD JAR '/appdata/connector-jars/flink-sql-connector-kafka-1.17.1.jar';
-- ADD JAR '/appdata/connector-jars/ververica-connector-kafka-1.15-vvr-6.0.2-3.jar';
-- ADD JAR '/appdata/connector-jars/ztnCustomerConnectors-1.0-SNAPSHOT.jar';
-- ADD JAR '/appdata/connector-jars/flink-connector-starrocks-1.2.8_flink-1.17.jar';
ADD JAR '/home/ztsauser/gateway_connectors/sql/connectors/upsert-kafka/flink-sql-connector-kafka-3.0.1-1.18.jar';
ADD JAR '/home/ztsauser/gateway_connectors/sql/formats/db-json/db-json-1.0.jar';
ADD JAR '/home/ztsauser/gateway_connectors/sql/catalogs/starrocks/flink-connector-starrocks-1.2.9_flink-1.18.jar';
ADD JAR '/home/ztsauser/gateway_connectors/sql/catalogs/doris/flink-doris-extend-connector-1.18-1.4.0.jar';

set execution.max-concurrent-checkpoints = 1;
set  execution.checkpointing.interval =60s;
set  execution.checkpointing.mode = 'EXACTLY_ONCE';
set  execution.checkpointing.min-pause = 20s;
set  execution.checkpointing.timeout = 60s;
set  execution.checkpointing.cleanup-mode = false;
set  execution.checkpointing.tolerable-failed-checkpoints = 30;
set  restart-strategy = fixed-delay;
set  restart-strategy.fixed-delay.attempts = 2147483647;
set  execution.checkpointing.alignment-timeout = 30s;
{regex_note}



CREATE TABLE kafka_reader(
    `_split_number_`  string,
    {pulsar_sql_detail}
    `__DORIS_SEQUENCE_TS_DT__` timestamp,
    `__SEQUENCE_FIELD__` bigint,
    `__DELETE_SIGN__` INT
) with (
    'connector' = 'kafka',
    'topic' = '{topic}', 
    'properties.bootstrap.servers' = 'alikafka-pre-cn-pe339q5gq00c-1-vpc.alikafka.aliyuncs.com:9092,alikafka-pre-cn-pe339q5gq00c-2-vpc.alikafka.aliyuncs.com:9092,alikafka-pre-cn-pe339q5gq00c-3-vpc.alikafka.aliyuncs.com:9092',
    'properties.group.id' = 'kafka_to_31sr__{sink_db}__{sink_table_prefix}_{source_table}',
    'properties.enable.auto.commit' = 'false',
    'properties.auto.offset.reset' = 'earliest',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'db-json',
    'db-json.database' = '{source_db}',   分库使用 | 连接,不填表示不区分库名
    'db-json.tablename' = '{source_table}' 注意正则
    -- 'db-json.tableNameRegrex' = ''  注意正则
);


CREATE TABLE sr_writer(
    `_split_number_`  string,
    {pulsar_sql_detail}
    __ETL_INSERT_TIME__	string,
    __DORIS_SEQUENCE_TS_DT__	timestamp,
    __SEQUENCE_FIELD__	BIGINT,
    __BSN_CREATE_USER__	string,
    __DELETE_SIGN__	INT,
    PRIMARY KEY (_split_number_{pri_key}) NOT ENFORCED

)WITH (
    'connector' = 'starrocks',
    'jdbc-url' = 'jdbc:mysql://10.240.18.3:19030',
    'load-url' = '10.240.18.3:18030;10.240.18.5:18030;10.240.18.7:18030',
    'database-name' = '{sink_db}',
    'table-name' = '{sink_table_prefix}_{source_table}',
    'username' = 'load_user',
    'password' = 'neBQ2MNs2n7dN2kf',
    'sink.properties.merge_condition' = '__SEQUENCE_FIELD__',
    'sink.properties.format' = 'json',
    'sink.properties.strip_outer_array' = 'true',
    'sink.properties.strict_mode' = 'true',
    'sink.semantic' = 'exactly-once',
    'sink.properties.max_filter_ratio' = '0',
    'sink.label-prefix' = '{sink_db}__{sink_table_prefix}_{source_table}_01_'
);

insert into sr_writer(
    _split_number_,
    {columns_with_enter},
    `__ETL_INSERT_TIME__`,
    `__DORIS_SEQUENCE_TS_DT__`,
    `__SEQUENCE_FIELD__`,
    `__BSN_CREATE_USER__`,
    `__DELETE_SIGN__`
) 
select
    `_split_number_`,
    {columns_with_enter},
    DATE_FORMAT(NOW(),'yyyy-MM-dd HH:mm:ss') as `__ETL_INSERT_TIME__`,
    `__DORIS_SEQUENCE_TS_DT__`,
    `__SEQUENCE_FIELD__`,
    '' as `__BSN_CREATE_USER__`,
    __DELETE_SIGN__
  from kafka_reader;
    '''

    # print(f'''{detail_flag}  >>>   {pulsar_sql_res}''')
    return kafka_sql_res



def get_sr_sql(datasource, source_db, source_table, comment, topic, pulsar_sql_str, sink_db,
                   sink_table_prefix, columns_with_enter, select_columns_with_enter, detail_flag,pri_key):
    detail_flag = True
    regex_str = ''
    regex_note = ''
    split_table_source = source_table
    like_options = ''
    regex_flag = str.__contains__(comment, '分表结构来源')
    if regex_flag:
        regex_str = "'db-json.tableNameRegrex' = '{}+(?=_[0-9]+)',"
        regex_note = '分表,请注意分表正则！！！！'
        if detail_flag:
            print(f'{sink_db}_{sink_table_prefix}_{source_table}   分表结构来源：{split_table_source}')
            print('>>>>>>    ', comment)
        split_table_source = str.replace(comment.split('分表结构来源')[1], ')', '')
        # print(f'{sink_db}_{sink_table_prefix}_{source_table}   分表结构来源：{split_table_source}')

    pulsar_sql_detail = pulsar_sql_str + ','

    source_catalog_str = ''
    if not detail_flag:
        like_options = f'''like source_catalog.{source_db}.{split_table_source}
(EXCLUDING OPTIONS)'''
        pulsar_sql_detail = ''
        source_catalog_str = f'''create catalog source_catalog with(
    'type' = 'ztn',
    'datasource' = '{datasource}',
    'env' = 'prod'
);'''

    doris_to_sr_sql = f'''
/*           {regex_note}
***********  {source_db}.{source_table}  ***************************
******作业名: doris_to_31sr__{sink_db}__{sink_table_prefix}_{source_table}  *********
*/
ADD JAR '/appdata/connector-jars/ztnCustomerConnectors-1.0-SNAPSHOT.jar';
ADD JAR '/appdata/connector-jars/flink-doris-extend-connector-1.17-1.4.0.jar';
ADD JAR '/appdata/connector-jars/flink-connector-starrocks-1.2.8_flink-1.17.jar';

set execution.max-concurrent-checkpoints = 1;
set  execution.checkpointing.interval = 60s;
set  execution.checkpointing.mode = 'EXACTLY_ONCE';
set  execution.checkpointing.min-pause = 20s;
set  execution.checkpointing.timeout = 180s;
set  execution.checkpointing.cleanup-mode = false;
set  execution.checkpointing.tolerable-failed-checkpoints = 30;
set  restart-strategy = fixed-delay;
set  restart-strategy.fixed-delay.attempts = 2147483647;
set  execution.checkpointing.alignment-timeout = 30s;
set 'parallelism.default' = '5';



 create catalog pro_doris with(
  'type' = 'ztn',
  'datasource' = 'doris_dev',
  'env' = '26_prod', 
  'parallelism.read' = 'true',
   'properties.doris.request.connect.timeout.ms' = '120000',
    'properties.doris.request.read.timeout.ms' = '120000',
    'properties.sink.properties.format' = 'json',
    'properties.sink.parallelism' = '8',
    'properties.sink.properties.read_json_by_line' = 'true',
    'properties.sink.enable-delete' = 'false',
    'properties.sink.properties.max_filter_ratio'  ='0',
    'properties.sink.properties.strict_mode'  = 'true',
    'properties.doris.batch.size' = '2048',
      'properties.sink.buffer-flush.max-rows' = '200000',
      'properties.sink.buffer-flush.max-bytes' = '104857600'
);

CREATE TABLE sr_writer(
    `_split_number_`  string,
    {pulsar_sql_detail}
    __ETL_INSERT_TIME__	string,
    __DORIS_SEQUENCE_TS_DT__	timestamp,
    __SEQUENCE_FIELD__	BIGINT,
    __BSN_CREATE_USER__	string,
    __DELETE_SIGN__	INT,
    PRIMARY KEY (_split_number_{pri_key}) NOT ENFORCED
)WITH (
    'connector' = 'starrocks',
    'jdbc-url' = 'jdbc:mysql://10.240.18.3:19030',
    'load-url' = '10.240.18.3:18030;10.240.18.5:18030;10.240.18.7:18030',
    'database-name' = '{sink_db}',
    'table-name' = '{sink_table_prefix}_{source_table}',
    'username' = 'load_user',
    'password' = 'neBQ2MNs2n7dN2kf',
    'sink.properties.merge_condition' = '__SEQUENCE_FIELD__',
    'sink.properties.format' = 'json',
    'sink.properties.strip_outer_array' = 'true',
    'sink.properties.strict_mode' = 'true',
    'sink.semantic' = 'exactly-once',
    'sink.properties.max_filter_ratio' = '0',
    'sink.label-prefix' = '{sink_db}__{sink_table_prefix}_{source_table}_02_'
);
insert into sr_writer(
    _split_number_,
    {columns_with_enter},
    `__ETL_INSERT_TIME__`,
    `__DORIS_SEQUENCE_TS_DT__`,
    `__SEQUENCE_FIELD__`,
    `__BSN_CREATE_USER__`,
    `__DELETE_SIGN__`
) 
select
    `_split_number_`,
    {columns_with_enter},
       `__ETL_INSERT_TIME__`,
    `__DORIS_SEQUENCE_TS_DT__`,
    `__SEQUENCE_FIELD__`,
    `__BSN_CREATE_USER__`,
    `__DELETE_SIGN__`
  from pro_doris.{sink_db}.{sink_table_prefix}_{source_table}
  where __DELETE_SIGN__ = 0;
    '''

    # print(f'''{detail_flag}  >>>   {pulsar_sql_res}''')
    return doris_to_sr_sql




def get_pulsar_sql(datasource, source_db, source_table, comment, pulsar_topic, pulsar_sql_str, sink_db,
                   sink_table_prefix, columns_with_enter, select_columns_with_enter, detail_flag):
    regex_str = ''
    regex_note = ''
    split_table_source = source_table
    like_options = ''
    regex_flag = str.__contains__(comment, '分表结构来源')
    if regex_flag:
        regex_str = "'cdc_tableName_regex' = '[A-Za-z_]+(?=_[0-9]+)',"
        regex_note = '分表,请注意分表正则！！！！'
        if detail_flag:
            print(f'{sink_db}_{sink_table_prefix}_{source_table}   分表结构来源：{split_table_source}')
            print('>>>>>>    ', comment)
        split_table_source = str.replace(comment.split('分表结构来源')[1], ')', '')
        # print(f'{sink_db}_{sink_table_prefix}_{source_table}   分表结构来源：{split_table_source}')

    pulsar_sql_detail = pulsar_sql_str + ','

    source_catalog_str = ''
    if not detail_flag:
        like_options = f'''like source_catalog.{source_db}.{split_table_source}
(EXCLUDING OPTIONS)'''
        pulsar_sql_detail = ''
        source_catalog_str = f'''create catalog source_catalog with(
    'type' = 'ztn',
    'datasource' = '{datasource}',
    'env' = 'prod'
);'''

    pulsar_sql_res = f'''
/*           {regex_note}
***********  {source_db}.{source_table}  ***************************
******作业名: {sink_db}_{sink_table_prefix}_{source_table}  *********
*/
set execution.max-concurrent-checkpoints = 1;
set  execution.checkpointing.interval =60s;
set  execution.checkpointing.mode = 'EXACTLY_ONCE';
set  execution.checkpointing.min-pause = 20s;
set  execution.checkpointing.timeout = 60s;
set  execution.checkpointing.cleanup-mode = false;
set  execution.checkpointing.tolerable-failed-checkpoints = 30;
set  restart-strategy = fixed-delay;
set  restart-strategy.fixed-delay.attempts = 2147483647;
set  execution.checkpointing.alignment-timeout = 30s;
{regex_note}

{source_catalog_str}

create catalog doris_dev with(
    'type' = 'ztn',
    'datasource' = 'doris_dev',
    'env' = 'prod'
);
CREATE TABLE source_pulsar (
    `_split_number_`  string,
    {pulsar_sql_detail}
    `__DORIS_SEQUENCE_TS_DT__` timestamp,
    `__SEQUENCE_FIELD__` bigint,
    `__DELETE_SIGN__` INT
) WITH (
    'connector' = 'cdc-pulsar',
    'serviceUrl' = 'pulsar://10.192.112.43:6650,10.192.112.44:6650,10.192.112.45:6650',
    'adminUrl' = 'http://10.192.112.43:8080,10.192.112.44:8080,10.192.112.45:8080',
    'topic' = 'persistent://dw/db_ods/{pulsar_topic}',   
    'subscriptionName' = '{sink_table_prefix}_{source_table}_当日日期',
    'subscriptionType' = 'Shared',
    'startupMode' = 'earliest',
     {regex_str}
     {regex_note}
    'only_table_Name' = '{source_table}',
    'updateMode' = 'append',
    'format' = 'json',
    'include_before' = 'false',
    'properties.pulsar.client.authPluginClassName' = 'org.apache.pulsar.client.impl.auth.AuthenticationToken',
    'properties.pulsar.client.authParams' = 'token:eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJldGwifQ.p7dG3a0Fy3qNJX7OQ6n7BHF5XFLB7WuzbyjMrcCi0AU'
){like_options};

CREATE TABLE sink_table  
WITH (
    'connector' = 'ztn-doris',
    'datasource' = 'doris_dev',
    'env' = 'prod',
    'table.identifier' = '{sink_db}.{sink_table_prefix}_{source_table}',
    'sink.properties.format' = 'json',
    'sink.properties.read_json_by_line' = 'true',
    'sink.enable-delete' = 'false',
    'sink.properties.max_filter_ratio'  ='0',
    'sink.properties.strict_mode'  = 'true',
    'sink.label-prefix' = '{sink_db}_{sink_table_prefix}_{source_table}_01_',
    'sink.properties.function_column.sequence_col' = '__SEQUENCE_FIELD__'
)like doris_dev.{sink_db}.{sink_table_prefix}_{source_table}
(EXCLUDING OPTIONS);

INSERT INTO sink_table
(
    `_split_number_`,
    {columns_with_enter},
    `__ETL_INSERT_TIME__`,
    `__DORIS_SEQUENCE_TS_DT__`,
    `__SEQUENCE_FIELD__`,
    `__BSN_CREATE_USER__`,
    `__DELETE_SIGN__`
)
select 
    `_split_number_`,
    {columns_with_enter},
    DATE_FORMAT(NOW(),'yyyy-MM-dd HH:mm:ss') as `__ETL_INSERT_TIME__`,
    `__DORIS_SEQUENCE_TS_DT__`,
    `__SEQUENCE_FIELD__`,
    'zt22150' as `__BSN_CREATE_USER__`,
    __DELETE_SIGN__
from source_pulsar;
    '''

    # print(f'''{detail_flag}  >>>   {pulsar_sql_res}''')
    return pulsar_sql_res


def execute_sql(ip, port, db, user, pwd, sql):
    conn = pymysql.connect(host=ip, port=port, user=user, password=pwd,
                           database=db, charset='utf8')
    mycursor = conn.cursor()
    mycursor.execute(sql)
    result = mycursor.fetchall()
    mycursor.close()
    conn.close()
    return result


def get_sql(datasource
            , source_table
            , source_db
            , sink_db
            , sink_table_prefix
            , server_id
            , columns
            , etl_user
            , size
            , comment
            , column_detail
            , pri_key
            , select_columns
            , pulsar_sql_str
            , pulsar_topic
            ):
    bucket = 5
    scheduler_sql = ''

    if size <= 100:
        bucket = math.ceil(size / 5)
    else:
        bucket = math.ceil(size / 10)
    flink_sql_res = ''
    seatunnel_sql_res = ''

    if bucket < 5:
        bucket = 5
    create_sql = f'''
    CREATE TABLE if not exists  {sink_db}.{sink_table_prefix}_{source_table} (
      `_split_number_` varchar(300)  COMMENT '来源库表键',
       {column_detail}
      `__ETL_INSERT_TIME__` datetime NULL COMMENT 'ETL插入时间',
      `__DORIS_SEQUENCE_TS_DT__` datetime NULL COMMENT '业务数据变化时间,毫秒数long，毫秒数',
      `__SEQUENCE_FIELD__` bigint comment '序列化更新字段',
      `__BSN_CREATE_USER__` string NULL COMMENT '表负责人',
      `__DELETE_SIGN__` int(11) not NULL  default '0' COMMENT '逻辑删除 0否1是'
    ) ENGINE=OLAP
    PRIMARY KEY(`_split_number_`{pri_key})
    COMMENT '{comment}'
    DISTRIBUTED BY HASH(`_split_number_`{pri_key}) BUCKETS {bucket}
    PROPERTIES (
    "replication_num" = "1",
    "storage_type" = "column",
    "enable_persistent_index" = "true"
);
        '''

    columns_with_enter = str.replace(columns, ',', ',\n    ')
    select_columns_with_enter = str.replace(select_columns, ',', ',\n    ')

    regex_str = ''
    regex_note = ''
    split_table_source = source_table
    regex_flag = str.__contains__(comment, '分表结构来源')
    if regex_flag:
        regex_str = "'cdc_tableName_regex' = '[A-Za-z_]+(?=_[0-9]+)',"
        regex_note = '分表,请注意分表正则！！！！'
        split_table_source = str.replace(comment.split('分表结构来源')[1], ')', '')
        # print(f'{sink_db}_{sink_table_prefix}_{source_table}   分表结构来源：{split_table_source}')

    flink_sql_res = f'''

/************{source_db}.{source_table}**********
*************并发数:{math.ceil(size / 50)}**********
*************表大小{size}GB  (小于1G都是1)**********/

set execution.max-concurrent-checkpoints = 1;
set  execution.checkpointing.interval = 180s;
set  execution.checkpointing.mode = 'EXACTLY_ONCE';
set  execution.checkpointing.min-pause = 60s;
set  execution.checkpointing.timeout = 600s;
set  execution.checkpointing.cleanup-mode = false;
set  execution.checkpointing.fail-on-error = 100;
set  execution.checkpointing.alignment-timeout = 60s;
{regex_note}

create catalog {datasource} with(
    'type' = 'ztn',
    'datasource' = '{datasource}',
    'env' = 'prod'
);
create catalog ods_doris_etl with(
    'type' = 'ztn',
    'datasource' = 'ods_doris_etl',
    'env' = 'prod'
);
CREATE TABLE sink_{sink_table_prefix}_{source_table}
 WITH (
    'connector' = 'ztn-doris',
    'datasource' = 'ods_doris_etl',
    'env' = 'prod',
    'table.identifier' = '{sink_db}.{sink_table_prefix}_{source_table}',
    'sink.properties.format' = 'json',
    'sink.properties.read_json_by_line' = 'true',
    'sink.enable-delete' = 'false',
    'sink.label-prefix' = '{sink_db}__{sink_table_prefix}_{source_table}_',
    'sink.properties.max_filter_ratio'  ='0',
    'sink.properties.strict_mode'  = 'true',
    'sink.properties.strip_outer_array' = 'false'
)like ods_doris_etl.{sink_db}.{sink_table_prefix}_{source_table}
(EXCLUDING OPTIONS);

CREATE TABLE source_{source_table}(
    db_name STRING METADATA FROM 'database_name' VIRTUAL,
    table_name STRING METADATA  FROM 'table_name' VIRTUAL,
    operation_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
    __operation_type__ INT METADATA FROM 'op_type' VIRTUAL
) WITH (
    'connector' = 'ztn-mysql-cdc',
    'datasource' = '{datasource}',
    'env' = 'prod',
    'database-name' = '{source_db}',
    'table-name' = '{source_table}',
    {regex_note}
    'server-id' = '{server_id}',
    'scan.startup.mode' = 'initial'
) like {datasource}.{source_db}.{source_table}
(EXCLUDING OPTIONS);

insert into sink_{sink_table_prefix}_{source_table}
(
    `_split_number_`, 
    {columns_with_enter}, 
    `__ETL_INSERT_TIME__`,
    `__DORIS_SEQUENCE_TS_DT__`,
    `__SEQUENCE_FIELD__`,
    `__BSN_CREATE_USER__`,
    `__DELETE_SIGN__`)
select 
    CONCAT(db_name,'.',table_name) as `_split_number_`,
    {select_columns_with_enter},
    DATE_FORMAT(NOW(),'yyyy-MM-dd HH:mm:ss') as `__ETL_INSERT_TIME__`,
    operation_ts as `__DORIS_SEQUENCE_TS_DT__` ,
    (UNIX_TIMESTAMP(DATE_FORMAT(NOW(),'yyyy-MM-dd HH:mm:ss')) * 100000000 + SECOND(NOW()) * 1000 + CAST(DATE_FORMAT(CURRENT_ROW_TIMESTAMP(), 'SSS') AS INT)) AS `__SEQUENCE_FIELD__`,
    '{etl_user}' as `__BSN_CREATE_USER__`,
    `__operation_type__` as `__DELETE_SIGN__` 
from source_{source_table};


        /************{source_db}.{source_table}*****************************/
'''

    seatunnel_sql_res = f'''
/********************文件名:  {sink_db}_{sink_table_prefix}_{source_table}**********************************/
*********************表  名： {sink_db}.{sink_table_prefix}_{source_table}*********************************/

env {{
  # You can set flink configuration here
  execution.parallelism = 1
  #execution.max-parallelism
  job.name = {sink_db}_{sink_table_prefix}_{source_table}
  execution.max-concurrent-checkpoints = 1
  execution.checkpoint.interval = 600000
  execution.checkpoint.mode = "exactly-once"
  execution.checkpoint.min-pause = 60000
  execution.checkpoint.timeout = 900000
  execution.checkpoint.cleanup-mode = false
  execution.checkpoint.fail-on-error = 100
  execution.checkpointing.alignment-timeout = 60000
}}

source {{
  JdbcSource {{
  parallelism = 1
  datasource = "{datasource}"
  query = "select '{source_db}.{source_table}' as `_split_number_`,{columns},now() as `__ETL_INSERT_TIME__`,now() as `__DORIS_SEQUENCE_TS_DT__`,UNIX_TIMESTAMP() * 1000 as `__SEQUENCE_FIELD__`,'{etl_user}' as `__BSN_CREATE_USER__`,0 as `__DELETE_SIGN__`  from {source_db}.{source_table}"
  batchNumber = "30000"
}}

}}

transform {{

}}

sink {{
     DorisSink {{
     datasource = "ods_doris_etl"
     database = "{sink_db}"
     #sink到doris的表名
     table = "{sink_table_prefix}_{source_table}"
     doris.format ="json"
     doris.strip_outer_array = "true"
     doris.strict_mode ="true"
     parallelism = 1
     dwUser="{etl_user}"
     batch_size = 30000
   }}
}}

/************************************************************************************/



        '''
    #     scheduler_sql = f'''
    # mysql -udev -pdhGN32sWkqEDyzli -P9030 -h10.192.112.14 -e "drop table if exists {sink_db}.{sink_table_prefix}_{source_table};
    # {create_sql}"
    # ${{SEATUNNEL_HOME}}/bin/start-seatunnel-flink.sh -c ods/lbu-onetable/{sink_db}/{sink_db}_{sink_table_prefix}_{source_table}.sql
    #
    #     '''
    scheduler_sql = f'''
${{SEATUNNEL_HOME}}/bin/start-seatunnel-flink.sh -c ods/lbu-onetable/{sink_db}/{sink_db}_{sink_table_prefix}_{source_table}.sql
        '''

    doris_to_sr = get_sr_sql(datasource, source_db, source_table, comment, pulsar_topic, pulsar_sql_str,
                                       sink_db,
                                       sink_table_prefix, columns_with_enter, select_columns_with_enter, True,pri_key)

    kafka_sql_detail = get_kafka_sql(datasource, source_db, source_table, comment, pulsar_topic, pulsar_sql_str,
                                       sink_db,
                                       sink_table_prefix, columns_with_enter, select_columns_with_enter, True,pri_key)

    pulsar_sql_catalog = get_pulsar_sql(datasource, source_db, source_table, comment, pulsar_topic, pulsar_sql_str,
                                        sink_db,
                                        sink_table_prefix, columns_with_enter, select_columns_with_enter, False)

    return [flink_sql_res, create_sql, scheduler_sql, seatunnel_sql_res, doris_to_sr, pulsar_sql_catalog,kafka_sql_detail]


def get_meta_data(source_db, table_lst, is_regix, regix_str):
    in_condition = "('" + ("'" + ',' + "'").join(table_lst) + "')"

    size_info_sql = f'''
    select
        TABLE_NAME,
        ceil(DATA_LENGTH/1024/1024/1024) as size,
        TABLE_COMMENT
    from information_schema.tables
    where TABLE_SCHEMA = '{source_db}'
    and TABLE_TYPE != 'VIEW'
    '''

    column_info_sql = f'''
    select
        TABLE_NAME,
        COLUMN_NAME,
        COLUMN_TYPE,
        NUMERIC_PRECISION,
        NUMERIC_SCALE,
        COLUMN_KEY,
        COLUMN_COMMENT 
    from information_schema.columns
    where TABLE_SCHEMA = '{source_db}'
    '''

    if table_lst[0] != 'all':
        size_info_sql += ' and TABLE_NAME in ' + in_condition
        column_info_sql += ' and TABLE_NAME in ' + in_condition
    column_info_sql += " order by TABLE_NAME,case when lower(COLUMN_KEY) = 'pri' then 0 else 1 end,ORDINAL_POSITION"
    # print(column_info_sql)
    column_res = execute_sql(host, port, source_db, user, pwd, column_info_sql)
    size_res = execute_sql(host, port, source_db, user, pwd, size_info_sql)

    table_info = {}
    split_table_info = set()
    for i in size_res:
        table_name = i[0]
        res_table_name = split_table_filter(table_name, is_regix, regix_str)
        res_table_size = i[1]
        split_cnt_res = 1
        table_comment = i[2]
        if (table_info.__contains__(res_table_name)):
            res_table_size += table_info.get(res_table_name).get('size', 0)
            split_cnt_res = table_info.get(res_table_name).get('split_cnt', 1) + 1
            table_comment = table_comment + f'({split_cnt_res}分表)'
            split_table_info.add(res_table_name)
        table_info[res_table_name] = {'size': res_table_size, 'table_comment': table_comment,
                                      'split_cnt': split_cnt_res}

    # test
    # print(table_info)
    # test_cnt = 0
    # for i in table_info:
    #     test_cnt += 1
    #     print(i + f">>>   {test_cnt}"  )

    # test

    pre_table = ''
    column_lst = []
    pri_lst = []
    detail_lst = []
    select_lst = []
    pulsar_sql_type_lst = []

    column_info = {}
    detail_info = {}
    pri_info = {}
    select_info = {}
    pulsar_sql_type = {}

    full_lst = []
    full_source_lst = []
    full_str = ''
    cur_table_column_cnt = 0
    column_res_table_name = ''
    pre_column_num = 0
    table_name = ''
    for j in column_res:
        table_name = j[0]
        column_res_table_name = split_table_filter(table_name, is_regix, regix_str)
        if pre_table == '':
            pre_table = j[0]
        column = j[1]
        cur_table_column_cnt += 1

        column_type = column_map(j[2])
        cast_str_flag = False
        if column_type == 'cast_to_varchar':
            cast_str_flag = True
            column_type = 'varchar(200)'
        column_comment = str.replace(j[6], "'", '')
        column_key = j[5]
        column_comment = str.replace(j[6], "'", '')

        if pre_table != table_name:
            detail_lst_tmp = detail_lst.copy()
            pri_lst_tmp = pri_lst.copy()
            select_lst_tmp = select_lst.copy()
            pulsar_sql_type_lst_tmp = pulsar_sql_type_lst.copy()

            pre_res_table_name = split_table_filter(pre_table, is_regix, regix_str)
            pre_column_num = column_info.get(pre_res_table_name, {}).get('column_cnt', 0)

            if pre_column_num <= cur_table_column_cnt:
                columns_str = ','.join(column_lst)
                column_info[pre_res_table_name] = {"column_info": columns_str, 'column_cnt': cur_table_column_cnt,
                                                   'table_name': pre_table}
                pri_info[pre_res_table_name] = pri_lst_tmp
                detail_info[pre_res_table_name] = detail_lst_tmp
                select_info[pre_res_table_name] = ','.join(select_lst_tmp)
                pulsar_sql_str = ',\n    '.join(pulsar_sql_type_lst_tmp)
                pulsar_sql_type[pre_res_table_name] = pulsar_sql_str
            pre_table = table_name
            cur_table_column_cnt = 0

            column_lst.clear()
            pri_lst.clear()
            detail_lst.clear()
            select_lst.clear()
            pulsar_sql_type_lst.clear()

        column_lst.append('`' + column + '`')
        if str.__contains__(str.lower(column_type), 'char') or str.lower(column_type) == 'string':
            if cast_str_flag:
                select_lst.append(f'CAST(`{column}` AS VARCHAR) as `{column}`')
            else:
                select_lst.append(f'TRIM(`{column}`) as `{column}`')

        else:
            select_lst.append(f'`{column}`')
        detail_lst.append(f"`{column}` {column_type} comment '{column_comment}',")

        if str.__contains__(str.lower(column_type), 'char'):
            pulsar_sql_type_lst.append(f'`{column}`\tstring')
        elif str.__contains__(str.lower(column_type), 'datetime'):
            pulsar_sql_type_lst.append(f'`{column}`\ttimestamp')
        elif str.__contains__(str.lower(column_type), '(') and (str.__contains__(str.lower(column_type), 'int')):
            tmp_type = column_type.split('(')[0]
            pulsar_sql_type_lst.append(f'`{column}`\t{tmp_type}')
        elif column_type == 'largeint':
            pulsar_sql_type_lst.append(f'`{column}`\tstring')
        else:
            pulsar_sql_type_lst.append(f'`{column}`\t{column_type}')

        if str.lower(column_key) == 'pri':
            pri_lst.append(column)

    pre_column_num = column_info.get(column_res_table_name, {}).get('column_cnt', 0)

    if pre_column_num <= cur_table_column_cnt:
        columns_str = ','.join(column_lst)
        column_info[column_res_table_name] = {"column_info": columns_str, 'column_cnt': cur_table_column_cnt,
                                              'table_name': table_name}
        pri_info[column_res_table_name] = pri_lst
        detail_info[column_res_table_name] = detail_lst
        select_info[column_res_table_name] = ','.join(select_lst)
        pulsar_sql_str = ',\n   '.join(pulsar_sql_type_lst)
        pulsar_sql_type[column_res_table_name] = pulsar_sql_str

    # print(column_info)
    # test_column_cnt = 0
    # for ii in column_info:
    #     test_column_cnt += 1
    #     print(f"column_cnt >>  {ii}  >>>   {test_column_cnt}")

    return [column_info, pri_info, detail_info, select_info, pulsar_sql_type, table_info]

def get_cdc_to_kafka(datasource, source_db, sink_db,topic):

    cdc_to_kafka_str = f'''
/*
***********  {source_db}***************************
******作业名:cdc_to_kafka__{sink_db}*********
*/
ADD JAR '/appdata/connector-jars/flink-doris-connector-1.17-1.4.0.jar';
ADD JAR '/appdata/connector-jars/flink-sql-connector-kafka-1.17.1.jar';
ADD JAR '/appdata/connector-jars/flink-sql-connector-pulsar-1.17.1.jar';
ADD JAR '/appdata/connector-jars/ververica-connector-kafka-1.15-vvr-6.0.2-3.jar';
ADD JAR '/appdata/connector-jars/ververica-connector-mysql-1.15-vvr-6.0.2-3-ztn.jar';
ADD JAR '/appdata/connector-jars/ztnCustomerConnectors-1.0-SNAPSHOT.jar';
set execution.max-concurrent-checkpoints = 1;
set  execution.checkpointing.interval =60s;
set  execution.checkpointing.mode = 'EXACTLY_ONCE';
set  execution.checkpointing.min-pause = 20s;
set  execution.checkpointing.timeout = 60s;
set  execution.checkpointing.cleanup-mode = false;
set  execution.checkpointing.tolerable-failed-checkpoints = 30;
set  restart-strategy = fixed-delay;
set  restart-strategy.fixed-delay.attempts = 2147483647;
set  execution.checkpointing.alignment-timeout = 30s;
CREATE TABLE binlog_reader (
     db STRING,
     `table` STRING,
     server_id BIGINT,
     snapshot STRING,
     file STRING,
     pos BIGINT,
     ts_ms BIGINT,
     connector STRING,
     op STRING,
     after STRING,
     ddl STRING,
     before STRING
 ) WITH (
    'connector'='db-cdc',
    'driver' = 'com.mysql.jdbc.Driver',
    'datasource'='{datasource}',
    'env'='prod',
    'timeZone' = 'Asia/Shanghai',
    'serverId' = '自己填', 自己填
    'databaseList' = '自己填', 根据实例信息自己填
    'tableList' = '',
    'splitSize' = '1024',
    'startupMode' = 'latest-offset',
    'debezium.table.exclude.list' = ''
  );


CREATE TABLE kafka_writer(
     db STRING,
     `table` STRING,
     server_id BIGINT,
     snapshot STRING,
     file STRING,
     pos BIGINT,
     ts_ms BIGINT,
     connector STRING,
     op STRING,
     after STRING,
     ddl STRING,
     before STRING
) WITH (
    'connector' = 'kafka',
    'properties.allow.auto.create.topics' = 'true',
    'topic' = '{topic}',
    'properties.bootstrap.servers' = 'alikafka-pre-cn-pe339q5gq00c-1-vpc.alikafka.aliyuncs.com:9092,alikafka-pre-cn-pe339q5gq00c-2-vpc.alikafka.aliyuncs.com:9092,alikafka-pre-cn-pe339q5gq00c-3-vpc.alikafka.aliyuncs.com:9092',
    'properties.group.id' = '{topic}',
    'properties.max.request.size' = '10485760',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'key.format' = 'raw',
    'key.fields' = 'table'
);
insert into kafka_writer select * from binlog_reader;
    '''
    return  cdc_to_kafka_str









if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="start batch flink session tasks")
    parser.add_argument('-H', '--host')
    parser.add_argument('-p', '--port', help='port')
    parser.add_argument('-u', '--user', help='user')
    parser.add_argument('-P', '--pwd', help='pasword')
    parser.add_argument('-d', '--datasource', help='datasource')
    parser.add_argument('-sd', '--sourcedb', help='source database')
    parser.add_argument('-dd', '--destdb', help='sink database')
    parser.add_argument('-l', '--lst', help='source tables')
    parser.add_argument('-sp', '--sinktableprefix', help='sink table prefix')
    parser.add_argument('-tp', '--topicName', help='pulsar topic')
    parser.add_argument('-is', '--isRegix', help='regixFlag', default='False')
    parser.add_argument('-rs', '--regixStr', help='regixStr', default='')

    parms = parser.parse_args()
    host = parms.host
    port = int(parms.port)
    user = parms.user
    pwd = parms.pwd
    # print(pwd)
    datasource = parms.datasource
    source_db = parms.sourcedb
    sink_db = parms.destdb
    sink_table_prefix = parms.sinktableprefix
    pulsar_topic = parms.topicName
    isR = parms.isRegix
    is_regix = False
    regix_str = ''
    if str.lower(isR) == 'true':
        is_regix = True
        regix_str = parms.regixStr

    table_lst = parms.lst.split(',')

    [column_info, pri_info, detail_info, select_info, pulsar_sql_type, table_info] = get_meta_data(source_db, table_lst,
                                                                                                   is_regix, regix_str)

    flink_sql_file_name = f'{sink_db}_1_flink_sql.sql'
    create_sql_file_name = f'{sink_db}_2_create_sql.sql'
    seatunnel_sql_file_name = f'{sink_db}_3_seatunnel_sql.sql'
    command_sql_file_name = f'{sink_db}_4_command_sql.sql'
    doris_to_sr_file_name = f'{sink_db}_5_doris_to_sr.sql'
    pulsar_sql_catalog_file_name = f'{sink_db}_6_pulsar_sql_catalog.sql'
    kafka_sql_file_name = f'{sink_db}_7_kafka_sql.sql'
    cdc_to_kafka_file_name = f'{sink_db}_8_cdc_to_kafka_sql.sql'

    flink_sql_writer = open(f"./output/{flink_sql_file_name}", "w+", encoding='utf8')
    create_sql_writer = open(f"./output/{create_sql_file_name}", "w+", encoding='utf8')
    seatunnel_sql_writer = open(f"./output/{seatunnel_sql_file_name}", "w+", encoding='utf8')
    schedule_sql_writer = open(f"./output/{command_sql_file_name}", "w+", encoding='utf8')
    doris_to_sr_writer = open(f"./output/{doris_to_sr_file_name}", "w+", encoding='utf8')
    pulsar_sql_catalog_writer = open(f"./output/{pulsar_sql_catalog_file_name}", "w+", encoding='utf8')
    kafka_sql_writer = open(f"./output/{kafka_sql_file_name}", "w+", encoding='utf8')
    cdc_to_kafka_writer = open(f"./output/{cdc_to_kafka_file_name}", "w+", encoding='utf8')




    begin_server = 100000
    full_str_rest = ''
    for i in table_info:
        single_table = table_info[i]
        size = single_table.get('size', 1)
        split_table_cnt = single_table.get('split_cnt', 1)
        single_column_info = column_info[i]
        columns = single_column_info.get('column_info', '')

        column_source = single_column_info.get('table_name', '')
        pri_lst = pri_info[i]
        select_columns = select_info[i]
        pri_key = ''
        if len(pri_lst) != 0:
            pri_key = ',' + ','.join(pri_lst)
        else:
            pri_key = ',' + columns

        column_detail_lst = detail_info[i]
        column_detail_str = '\n\t'.join(column_detail_lst)
        # step = math.ceil(size/50)
        step = 1
        server_id = f'{begin_server + 1}-{begin_server + 2 + step}'
        begin_server = begin_server + 2 + step
        pulsar_sql_str = pulsar_sql_type[i]
        comment = single_table['table_comment']
        if split_table_cnt > 1:
            comment = comment + f'(分表结构来源{column_source})'
        str1 = get_sql(datasource
                       , i
                       , source_db
                       , sink_db
                       , sink_table_prefix
                       , server_id
                       , columns
                       , 'zt22150'
                       , size
                       , comment
                       , column_detail_str
                       , pri_key
                       , select_columns
                       , pulsar_sql_str
                       , pulsar_topic
                       )


        flink_sql_writer.write(str1[0])
        seatunnel_sql_writer.write(str1[3])
        schedule_sql_writer.write(str.replace(str1[2], '`', '\`'))
        create_sql_writer.write(str1[1])
        doris_to_sr_writer.write(str1[4])
        pulsar_sql_catalog_writer.write(str1[5])

        kafka_sql_detail_str = str1[6]
        kafka_sql_writer.write(kafka_sql_detail_str)

    cdc_to_kafka_str = get_cdc_to_kafka(datasource, source_db, sink_db,pulsar_topic)
    cdc_to_kafka_writer.write(cdc_to_kafka_str)

    flink_sql_writer.close()
    create_sql_writer.close()
    seatunnel_sql_writer.close()
    schedule_sql_writer.close()
    doris_to_sr_writer.close()
    pulsar_sql_catalog_writer.close()
    kafka_sql_writer.close()
    cdc_to_kafka_writer.close()

    cur_path = os.getcwd() + os.sep + 'output'
    print('结果路径:')
    print(cur_path + os.sep + flink_sql_file_name)
    print(cur_path + os.sep + create_sql_file_name)
    print(cur_path + os.sep + seatunnel_sql_file_name)
    print(cur_path + os.sep + command_sql_file_name)
    print(cur_path + os.sep + doris_to_sr_file_name)
    print(cur_path + os.sep + pulsar_sql_catalog_file_name)
    print(cur_path + os.sep + kafka_sql_file_name)
    print(cur_path + os.sep + cdc_to_kafka_file_name)




