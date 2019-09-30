package com.readailib.hbase.utils;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import static org.junit.Assert.*;

@Slf4j
public class HBaseUtilsTest {

    @Test
    public void scan() {
        /** 这个将会在HBase中设置为表的名字*/
        String tablename = "wordcount";
        HBaseUtils.scan(tablename);
    }
}