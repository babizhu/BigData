package com.bbz.bigdata.mapreduce.transform.file2hbase;

import org.apache.hadoop.io.Text;

/**
 * Created by liu_k on 2016/5/9.
 * 用于解析轨迹记录
 *
 */
public class TrackRecordParser{

    public int getX(){
        return x;
    }

    public void setX( int x ){
        this.x = x;
    }

    public int getY(){
        return y;
    }

    public void setY( int y ){
        this.y = y;
    }

    public String getBadage(){
        return badage;
    }

    public void setBadage( String badage ){
        this.badage = badage;
    }

    public String getTime(){
        return time;
    }

    public void setTime( String time ){
        this.time = time;
    }

    private int x,y;
    private String badage;
    private String time;
    public void parse( Text value ){
        String line = value.toString();
        String[] split = line.split( "\t" );
        x = Integer.parseInt( split[1] );
        y = Integer.parseInt( split[2] );

        badage = split[7];
        time = split[6];
//        validTemperature = true;
//        if( line.length() < 8 ) {
//            validTemperature = false;
//            return;
//        }
//
//        airTemperature = Integer.parseInt( line.substring( 8, line.length() ) );
//        year = Integer.parseInt( line.substring( 0, 4 ) );

    }

    public boolean isValidTemperature(){

        return true;
    }
}
