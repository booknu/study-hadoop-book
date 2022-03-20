// cc MaxTemperature Application to find the maximum temperature in the weather dataset
// vv MaxTemperature
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 실제 맵리듀스 Job 을 실행시켜볼 수 있다.
 */
public class MaxTemperature {

  public static void main(String[] args) throws Exception {
    System.out.println(System.getProperty("user.dir"));

    if (args.length != 2) {
      System.err.println("Usage: MaxTemperature <input path> <output path>");
      System.exit(-1);
    }

    // Job 을 수행하는방법에 대한 명세서를 결정한다.
    Job job = new Job();
    // 하둡은 클러스터 머신들에게 코드의 JAR 파일을 배포한다.
    // JAR 파일 이름을 명시적으로 지정할 수도 있지만, 이렇게 클래스 하나를 지정하면 해당 클래스를 포함한 JAR 파일을 클러스터에 배치해준다.
    job.setJarByClass(MaxTemperature.class);
    job.setJobName("Max temperature");

    // Job 의 입출력 경로를 지정한다.
    // 입력 경로가 여러 경로라면, 패턴으로 지정하거나 함수를 여러번 호출한다.
    // 출력 경로는 Job 을 수행하는 시점에는 절대 존재해서는 안 되며, 만약 그런 경우 하둡은 에러를 발생시켜 Job 을 실행하지 않는다.
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // Map, Reduce 를 위한 구현 클래스 타입을 지정한다.
    job.setMapperClass(MaxTemperatureMapper.class);
    job.setReducerClass(MaxTemperatureReducer.class);

    // Reduce 의 출력 타입을 지정한다.
    // 실제 Reducer 클래스의 출력 타입과 반드시 일치해야한다. (컴파일 타임에서 확인해주지 않기 때문에 조심)
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    // Mep 의 출력 타입은 기본적으로 Reduce 의 출력 타입과 같도록 설정된다.
    // 만약 다르다면, 직접 타입을 지정해줘야 한다.
//    job.setMapOutputKeyClass(Text.class);
//    job.setMapOutputValueClass(IntWritable.class);

    // 입력 포맷은 기본적으로 TextInputFormat 을 사용한다.
//    job.setInputFormatClass(TextInputFormat.class);

    // Job 을 실행하고 기다리며, 실행이 성공한 경우 true 를 반환한다.
    // 파라미터의 플래그 값은 실행 중 콘솔로 상황을 보여줄지 여부를 결정할 수 있다.
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
// ^^ MaxTemperature
