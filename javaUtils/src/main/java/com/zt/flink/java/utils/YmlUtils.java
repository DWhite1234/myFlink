package com.zt.flink.java.utils;

import com.esotericsoftware.minlog.Log;
import io.minio.MinioClient;
import io.minio.errors.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;

/**
 * @author zt
 */
@Slf4j
public class YmlUtils {

    /**
     * 读取minio的文件
     * @param url
     * @param bucket
     * @param bucketFilePath
     * @return
     */
    public static Map<String,String> getMinioUrl(String url,String bucket,String bucketFilePath) {
        InputStream inputStream = null;
        try {
            MinioClient minioClient = new MinioClient(url);
            inputStream = minioClient.getObject(bucket, bucketFilePath);
        } catch (Exception ignored){
            log.info("minio路径不正确,url:{},bucket:{},bucketFilePath:{}",url,bucket,bucketFilePath);
        }
        Yaml yaml = new Yaml();
        Object load = yaml.load(inputStream);
        return (Map<String, String>) load;
    }

    /**
     * 读取本地文件
     * @param url
     * @return
     */
    public static Map<String,String> getLocalPath(String url) {
        FileInputStream inputStream = null;
        try {
            inputStream = new FileInputStream(url);
        } catch (Exception e) {
            log.info("本地文件路径不正确:{}",url);
        }
        Yaml yaml = new Yaml();
        Object load = yaml.load(inputStream);
        return (Map<String, String>) load;
    }

    /**
     * 读取resource目录下的文件
     * @param name
     * @return
     */
    public static Map<String,String> getResourceFile(String name) {
        InputStream inputStream = null;
        try {
            inputStream = YmlUtils.class.getClassLoader().getResourceAsStream(name);
        } catch (Exception e) {
            log.info("resource目录下不存在指定文件:{}",name);
        }
        Yaml yaml = new Yaml();
        Object load = yaml.load(inputStream);
        return (Map<String, String>) load;
    }
}
