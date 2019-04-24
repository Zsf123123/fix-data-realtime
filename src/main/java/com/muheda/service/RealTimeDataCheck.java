package com.muheda.service;


import com.muheda.Main;

import com.muheda.domain.LngAndLat;
import com.muheda.realTimeDataRevice.RealTimeDataSource;
import com.muheda.utils.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.broadcast.Broadcast;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


/**
 * @desc  实时数据的检测与校验
 */
public class RealTimeDataCheck {


    // 需要被接收的数据的列族
    private  static  String reviceFamily =  ReadProperty.getConfigData("hbase.basicData.family");

    // 用于切分数据的分隔符
    private  static  String  regex = ReadProperty.getConfigData("current.data.separator.regex");

    /**
     * @desc  校验数据的准确性,处理传过来的单条数据,只是判断数据是否合法,是否是脏数据
     * @param record
     * @return
     */
   public static  boolean  checkDataLegal(ConsumerRecord<String,String>  record){

       //目前只接受basic的数据
       if (!record.key().equals(reviceFamily)) {
           return false ;
       }

       //获取该条数据的值
       String value = record.value();

       //将数据按照分隔符进行分割开来
       String[] basic = value.split(regex);

       if (basic.length != 16) {
           return false;
       }


       //获取设备号，经纬度，时间
       String deviceId = basic[0];

       String lng = basic[2];

       String lat = basic[3];

       String currentTime = basic[1];


       //如果设备号,经度，纬度，时间不为空；并且经纬度是在中国境内。时间是一个合法的时间，否则舍弃这个数据
       if(StringUtil.isEmpty(deviceId) && CheckDataUtils.validatelLng(lng) && CheckDataUtils.validatelLat(lat) && CheckDataUtils.isRightTime(currentTime)){

           //只有满足了上述的这些条件之后，才可以返回去。算是数据校验成功
           return true;

       }else{

           return false;
       }

   }



    /**
     * @desc 如果不是脏数据，则对设备的缓存信息进行更新操作，如果在更新的过程中遇到有的数据可以行程一个行程的话那么就可以进行数据修复和存储了
     */
    public static void  updataDeviceInfo(ConsumerRecord<String,String> record) throws CloneNotSupportedException {


        LngAndLat lngAndLat = orignToObject(record);

        String deviceId = lngAndLat.getDeviceId();

        Map<String, Map<String,Object>> cacheArea = RealTimeDataSource.cacheArea.getValue();

        Map<String, Object> deviceInfo = cacheArea.get(deviceId);


        // 初始化
        if( deviceInfo == null){

            Map<String, Object> map = new HashMap<>();

            List<LngAndLat> route = new LinkedList<>();

            route.add(lngAndLat);

            map.put("from", lngAndLat);
            map.put("to", lngAndLat);
            map.put("route",route);

            cacheArea.put( deviceId ,map);
        }


        if (deviceInfo != null) {


            double distance = MapUtils.getDistance(lngAndLat.getLng(), lngAndLat.getLat(), ((LngAndLat) deviceInfo.get("to")).getLng(), ((LngAndLat) deviceInfo.get("to")).getLat());

            int timeDif = DateUtils.getDiffDate(lngAndLat.getDate(), ((LngAndLat) deviceInfo.get("to")).getDate(), 1);

            //如果此次的时间与上次的时间之差大于十分钟，则进行行程的分段处理,将之前的行程作为一个完整的行程
            //或者此次的点与上次的点之间的距离大于5公里
            if( timeDif > 10 || distance > 5) {

                //将行程传递数据修复
                List route = (List) deviceInfo.get("route");


                //取出设备的设备号
                RealTimeDataRepair.startFixRoad(route, deviceId);


                //修复之后并更新相关数据
                route.clear();

                deviceInfo.put("from",lngAndLat);
                deviceInfo.put("to",lngAndLat);
                deviceInfo.put("route",new LinkedList<LngAndLat>().add(lngAndLat));




            }else {

               // 如果不满足上述条件，则对数据进行更新
               deviceInfo.put("route",((List<LngAndLat>)deviceInfo.get("route")).add(lngAndLat));
               deviceInfo.put("to",lngAndLat);


            }



        }




    }



    /**
     * @desc 将传过来的数据封装成对象
     * @param record
     * @return
     */
   public static LngAndLat orignToObject(ConsumerRecord<String,String> record){


       String[] split = record.value().split(regex);

       String deviceId  = split[0];

       String lng= split[2];

       String lat = split[3];

       String time = split[1];

       LngAndLat lngAndLat = new LngAndLat(deviceId,Double.parseDouble(split[2]), Double.parseDouble(split[3]), DateUtils.timeFormat(split[1]));

       return  lngAndLat;

   }





}


















