// cc MaxTemperatureReducer Reducer for maximum temperature example
// vv MaxTemperatureReducer
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reduce 함수는 Reducer 추상클래스를 구현하는 것으로 정의한다.
 * Mapper 와 비슷하게 제네릭으로 KEYIN, VALUEIN, KEYOUT, VALUEOUT 타입을 지정할 수 있다.
 */
public class MaxTemperatureReducer
  extends Reducer<Text, IntWritable, Text, IntWritable> {

  /**
   * reduce 메소드로 입력 데이터를 취합해 reduce 하는 로직을 구현할 수 있다.
   *
   * @param key Map 에서 출력된 key
   * @param values Map 에서 출력된 value 를 key 를 기준으로 그룹핑한 데이터.
   * @param context 출력을 위한 객체
   */
  @Override
  public void reduce(Text key, Iterable<IntWritable> values,
      Context context)
      throws IOException, InterruptedException {

    // 입략된 데이터를 취합한다.
    int maxValue = Integer.MIN_VALUE;
    for (IntWritable value : values) {
      maxValue = Math.max(maxValue, value.get());
    }
    // context 를 통해 취합된 데이터를 출력한다.
    context.write(key, new IntWritable(maxValue));
  }
}
// ^^ MaxTemperatureReducer
