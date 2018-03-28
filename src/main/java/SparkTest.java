import com.alibaba.fastjson.JSON;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;
import java.util.stream.LongStream;



/**
 * Created By @author songc
 * on 2017/11/4
 */
public class SparkTest implements Serializable{

    private String family="info";

    private byte[] qContent = Bytes.toBytes("content");

    public static void main(String[] args) {
        String appName =args[0];
        String tableName = args[1];
        String resultPath = args[2];
        int numExe = Integer.parseInt(args[3]);
        SparkTest sparkTest = new SparkTest();
        SparkConf conf = new SparkConf().setAppName(appName);
        JavaSparkContext sc = new JavaSparkContext(conf);
        Configuration config= HBaseConfiguration.create();
        config.set("hbase.roodir","hdfs://hadoop-hbase:9000/hbase");
        config.set("hbase.zookeeper.quorum","hadoop-hbase,hadoop-hbase-1,hadoop-hbase-2");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set(TableInputFormat.INPUT_TABLE, tableName);
        config.set(TableInputFormat.SCAN_COLUMN_FAMILY, sparkTest.family);
        JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = sc.newAPIHadoopRDD(config, TableInputFormat.class,
                ImmutableBytesWritable.class, Result.class);
        JavaPairRDD<String, byte[]> files = hbaseRDD.mapToPair(tuple2 -> {
            String rowKey = new String(tuple2._2.getRow()).substring(3);
            byte[] content = tuple2._2.getValue(sparkTest.family.getBytes(), sparkTest.qContent);
            return new Tuple2<>(rowKey, content);
        }).sortByKey(true,numExe);
        List<double[]> result = files.map(tuple2 -> {
            TiffImage image = new TiffImage(tuple2._2,tuple2._1);
            return image.getAllRegionGrayAverage(50,50);
        }).collect();
        JavaRDD<double[]> f = sc.parallelize(ImageUtil.getTranspose(result));
        f.map(s->{
            double[] x = LongStream.rangeClosed(1, s.length).asDoubleStream().toArray();
            return ExponentFitUtil.getOneExpFuncValue(SignalFit.fitOneExponent(x,s),x);
        }).map(JSON::toJSONString).saveAsTextFile("/Result/"+resultPath );
    }
}
