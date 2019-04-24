package com.muheda;


import com.muheda.realTimeDataRevice.RealTimeDataSource;

/**
 * @desc 实时标签系统的入口
 *
 */
public class Main {

    public static void main(String[] args) {

        RealTimeDataSource.startReviceKafkaData();

    }

}