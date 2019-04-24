package com.muheda.realTimeDataRevice;


import clojure.lang.Obj;
import com.muheda.service.RealTimeDataCheck;
import com.muheda.utils.ReadProperty;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import java.sql.Connection;
import java.util.*;

/**
 * @desc 这一部分是实时的获取kafka中的数据源来进行数据的修复工作
 *
 *
 */
public class RealTimeDataSource {

    private static Logger logger = Logger.getLogger(RealTimeDataSource.class);

    private static String servers;
    private static String groupId;
    private static String topies;
    private static String projectName = null;
    private static String type = null;
    private static String regex = null;
    private static String appName = null;
    private static  boolean bool = false;



    //用于缓存行程信息 将缓存信息转化成 广播变量
    public static  volatile Broadcast<Map<String, Map<String, Object>>> cacheArea = null;


    static {
        type = ReadProperty.getConfigData("current.data.separator.type");
        regex = ReadProperty.getConfigData("current.data.separator.regex");

        servers = ReadProperty.getConfigData("bootstrap.servers");
        groupId = ReadProperty.getConfigData("group.id");
        topies = ReadProperty.getConfigData("topies");
        projectName = ReadProperty.getConfigData("hbase.current.project");
        appName = ReadProperty.getConfigData("appName");

    }


    public  static void startReviceKafkaData() {
        //构建sparkConf参数
//        SparkConf sparkConf = new SparkConf().setAppName(appName).set("spark.testing.memory", "47185920000").setMaster("local[18]");
        SparkConf sparkConf = new SparkConf().setAppName(appName);
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        //构建sparkContext对象
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        //设置spark日志输出级别
        jsc.setLogLevel("WARN");
        //构建StreamingContext对象
        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(5));
        //Kafka相关参数配置
        Map<String, Object> kafkaParams = new HashMap<>(16);
        kafkaParams.put("bootstrap.servers", servers);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", groupId);
        //设置超时时间为100S
        kafkaParams.put("session.timeout.ms", "100000");
        //获取offset的规则，获取的是最新的消息，如果有offset值获取的是offset值，如果没有offset值，获取到的事最新的消息
        kafkaParams.put("auto.offset.reset", "earliest");
        //是否自动确认offset，因为手动提交offset，所以设置自动提交为false,如果设置自动提交，需设置自动提交时间间隔
        kafkaParams.put("enable.auto.commit", false);
        //spark从kafka读取的topic的集合信息
        Set<String> topicsSet = new HashSet<>(Arrays.asList(topies.split(",")));
        //使用spark直接读去kafka的方式，direct的方式
        JavaInputDStream<ConsumerRecord<String, String>> recordDStream = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topicsSet, kafkaParams)
        );

        Map<String, Map<String,Object>> map = new HashMap<>();


        //将广播变量缓存区进行赋值
        cacheArea = jsc.broadcast(map);


        recordDStream.foreachRDD((VoidFunction<JavaRDD<ConsumerRecord<String, String>>>) recordJavaRDD -> recordJavaRDD.foreachPartition(new VoidFunction<Iterator<ConsumerRecord<String, String>>>() {
            @Override
            public   void call(Iterator<ConsumerRecord<String, String>> javaRdd) throws Exception {


                while (javaRdd.hasNext()) {
                    ConsumerRecord<String, String> record = javaRdd.next();

                        if (type != null) {
                            //从配置文件中读取出此项目使用的分割方式
                            switch (type) {

                                case "separator":

                                 //针对歌图云镜的基础数据
                                 if( "basic".equals(record.key())){

                                     //数据校验
                                     boolean bool = RealTimeDataCheck.checkDataLegal(record);

                                     // 如果数据没有问题
                                     if(bool){

                                         RealTimeDataCheck.updataDeviceInfo(record);

                                     }


                                 }


                                case "json":


                            }

                        }
                }
            }
        }));

        //存储kafka的偏移量
        recordDStream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
            @Override
            public void call(JavaRDD<ConsumerRecord
                    <String, String>> rdd) {
                OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                ((CanCommitOffsets) recordDStream.inputDStream()).commitAsync(offsetRanges);
            }
        });

        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


}
