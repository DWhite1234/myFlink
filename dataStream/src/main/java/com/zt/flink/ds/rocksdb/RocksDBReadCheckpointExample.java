package com.zt.flink.ds.rocksdb;

import org.rocksdb.*;

public class RocksDBReadCheckpointExample {
    public static void main(String[] args) throws RocksDBException {
        String dbPath = "/Users/zhongtao/disk/大数据组件安装包/rocksdb-8.0.0/data";
        String checkpointPath = "/Users/zhongtao/disk/develop/ck/20230417/2ac18d84e8985a238be1893155fd4b7b";

        try (RocksDB rocksDB = RocksDB.openReadOnly(new Options(), dbPath)) {
            try (Checkpoint checkpoint = Checkpoint.create(rocksDB)) {
                checkpoint.createCheckpoint(checkpointPath);
                System.out.println("Checkpoint created at: " + checkpointPath);
            }
        }

        // 读取 chk 文件中的数据
        try (RocksDB checkpointDB = RocksDB.openReadOnly(new Options(), checkpointPath)) {
            // 进行你的操作，例如使用 get、put、iterator 等方法
            byte[] value = checkpointDB.get("key".getBytes());
            System.out.println("Value for key 'key': " + new String(value));
        }
    }
}
