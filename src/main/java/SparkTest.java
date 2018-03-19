import com.alibaba.fastjson.JSON;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.stream.LongStream;


/**
 * Created By @author songc
 * on 2017/11/4
 */
public class SparkTest implements Serializable{

    private String tableName="spark-test";

    private String family="info";

    private byte[] qParentId = Bytes.toBytes("parentId");
    private byte[] qName = Bytes.toBytes("name");
    private byte[] qContent = Bytes.toBytes("content");

    public static void main(String[] args) {
        SparkTest sparkTest = new SparkTest();
        String appName = "test";
        String master = "spark://hadoop-hbase:7077";
        SparkConf conf = new SparkConf().setAppName(appName).setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Configuration config;
        config = HBaseConfiguration.create();
        config.set("hbase.roodir","hdfs://hadoop-hbase:9000/hbase");
        config.set("hbase.zookeeper.quorum","hadoop-hbase");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.client.keyvalue.maxsize", "524288000");
        config.set(TableInputFormat.INPUT_TABLE, sparkTest.tableName);
        config.set(TableInputFormat.SCAN_COLUMN_FAMILY, sparkTest.family);
        JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = sc.newAPIHadoopRDD(config, TableInputFormat.class,
                ImmutableBytesWritable.class, Result.class);
        List<double[]> result = hbaseRDD.map(tuple2 -> {
            String rowKey = new String(tuple2._2.getRow());
            TiffImage image = new TiffImage(tuple2._2.getValue(sparkTest.family.getBytes(), sparkTest.qContent), rowKey);
            return image.getAllRegionGrayAverage(400,400);
        }).collect();
        JavaRDD<double[]> f = sc.parallelize(ImageUtil.getTranspose(result));
        f.map(s->{
            double[] x = LongStream.rangeClosed(1, s.length).asDoubleStream().toArray();
            return ExponentFitUtil.getDualExpFuncValue(SignalFit.fitDualExponent(x,s),x);
        }).map(JSON::toJSONString).saveAsTextFile("result2");


    }

}
