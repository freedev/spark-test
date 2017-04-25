package spark;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SparkTest {

  public static void main(String[] args) throws URISyntaxException {

    URL resource = SparkTest.class.getResource("/emp.json");

    System.out.println(resource.toString());
    Path path = Paths.get(resource.toURI());

    System.out.println(path.toString());

    final List<Map<String, String>> jsonData = new ArrayList<>();

    SparkConf conf = new SparkConf().setMaster("local[8]").setAppName("myApp");
    
    JavaSparkContext sc = new JavaSparkContext(conf);
    
    SQLContext sqlContext = new SQLContext(sc);
    // sqlContext
    
     DataFrame df = sqlContext.read()
     .json(path.toString());
     JavaRDD<String> rdd = df.repartition(1)
     .toJSON()
     .toJavaRDD();


    ObjectMapper objectMapper = new ObjectMapper();

    rdd.foreach(new VoidFunction<String>() {
      @Override
      public void call(String line) {
        try {
          
          jsonData.add(objectMapper.readValue(line, Map.class));
          System.out.println(Thread.currentThread()
                                   .getName());
          System.out.println("List size: " + jsonData.size());
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    });

    List<Map<String, Object>> list = 
        rdd.map(new org.apache.spark.api.java.function.Function<String, Map<String, Object>>() {
          @Override
          public Map<String, Object> call(String line) throws Exception {
            TypeReference<Map<String, Object>> typeRef = new TypeReference<Map<String, Object>>() {};
            Map<String, Object> rs = objectMapper.readValue(line, typeRef);
            return rs;
          }
        }).collect();

    System.out.println(Thread.currentThread()
                             .getName());
    System.out.println("List size: " + list.size());

  }

}
