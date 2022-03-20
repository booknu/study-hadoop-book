// cc MaxTemperatureMapper Mapper for maximum temperature example
// vv MaxTemperatureMapper
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Map 함수는 Mapper 추상클래스를 구현하는 것으로 정의된다.
 * Mapper 클래스는 제네릭으로 KEYIN, VALUEIN, KEYOUT, VALUEOUT 을 받고, 각각이 입출력 key-value 타입을 지정한다.
 * 최적화된 직렬화를 위해 자체적으로 사용하는 primitive type 셋을 지원한다. (LongWritable, Text, IntWritable)
 */
public class MaxTemperatureMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

  private static final int MISSING = 9999;

  /**
   * map 메소드로 입력된 데이터를 기반으로 Map 의 출력을 하는 로직을 구현할 수 있다.
   *
   * @param key KEYIN 타입의 key 입력
   * @param value VALUEIN 타입의 value 입력
   * @param context 출력을 위한 객체
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public void map(LongWritable key, Text value, Context context)
          throws IOException, InterruptedException {

    // 입력된 key-value 에서 데이터 파싱 후
    String line = value.toString();
    String year = line.substring(15, 19);
    int airTemperature;
    if (line.charAt(87) == '+') { // parseInt 에서 + 기호는 좋아하지 않기 떄문에 빼고 처리해준다.
      airTemperature = Integer.parseInt(line.substring(88, 92));
    } else {
      airTemperature = Integer.parseInt(line.substring(87, 92));
    }
    String quality = line.substring(92, 93);
    if (airTemperature != MISSING && quality.matches("[01459]")) {
      context.write(new Text(year), new IntWritable(airTemperature)); // key-value 를 출력한다.
    }
  }
}
// ^^ MaxTemperatureMapper
