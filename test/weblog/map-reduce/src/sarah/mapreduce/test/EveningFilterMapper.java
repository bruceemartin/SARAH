package sarah.mapreduce.test;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EveningFilterMapper extends Mapper<LongWritable, Text, Text, Text> {

	public static List<String> months = Arrays.asList("Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec");

  /**
   * Example input line:
   * 96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] "GET /cat.jpg HTTP/1.1" 200 12433
   *
   */
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    /*
     * Split the input line into space-delimited fields.
     */
    String[] fields = value.toString().split(" ");
    
    if (fields.length > 3) {
            
      /*
       * The fourth field contains [dd/Mmm/yyyy:hh:mm:ss].
       * Split the fourth field into "/" delimited fields.
       * The second of these contains the month.
       */
      String[] dtFields = fields[3].split("/");
      if (dtFields.length > 2) {
    	String theMonth = dtFields[1];
        String[] yearTime = dtFields[2].split(":");
        String hourStr = yearTime[1];
        int hour = Integer.parseInt(hourStr);
        /* check if it's a valid month and an evening hour */
        if (months.contains(theMonth) && hour>=12)
        	context.write(new Text(theMonth), value);
      }
    }
  }
}