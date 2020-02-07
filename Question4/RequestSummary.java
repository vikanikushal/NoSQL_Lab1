import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.mortbay.log.Log;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;



public class RequestSummary {

	
	public static class Summary {
		    int requests;
		    int downloadSize;

		    Summary() {
			requests=0;
			downloadSize=0;
		    }

		    Summary(int requests, int downloadSize) {
			this.requests = requests;
			this.downloadSize = downloadSize;
		    }

		    public int getDownloadSize() {
			return downloadSize;
		    }

		    public void setDownloadSize(int downloadSize) {
			this.downloadSize = downloadSize;
		    }

		    public int getRequests() {
			return requests;
		    }

		    public void setRequests(int requests) {
			this.requests = requests;
		    }
		}
		
	public static class RequestSummaryMapper extends Mapper<LongWritable, Text, Text, Summary> {

	public String getStringBetweenTwoChars(String input, String startChar, String endChar) {
		try {
			int start = input.indexOf(startChar);
			if (start != -1) {
				int end = input.indexOf(endChar, start + startChar.length());
				if (end != -1) {
					return input.substring(start + startChar.length(), end);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return input; // return null; || return "" ;
	}

	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		String timeStr = StringUtils.substringBetween(line, "[", "]");
		Log.debug("msg", timeStr);
		String month = timeStr.substring(3, 5);
		String year = timeStr.substring(7, 10);
		String combined = month + "-" + year;
		Log.debug("combined", combined);
		Summary summary = new Summary();
		summary.setRequests(1);
		int bytes = 0;
		if (line.contains("GET")) {
			int index = line.indexOf("1.1");
			String temp = "";
			index += 9;
			temp += line.charAt(index);
			while (line.charAt(index++) != '\n') {
				temp += line.charAt(index);
			}
			bytes = Integer.valueOf(temp);
		}
		summary.setDownloadSize(bytes);
		context.write(new Text(combined), summary);
	}

	}
	
	public static class RequestSummaryReducer extends Reducer<Text, Summary, Text, Summary> {

	public void reduce(Text key, Iterable<Summary> values, Context context) throws IOException, InterruptedException {

		int counter = 0;
		int downloadSize = 0;
		Summary temp;
		for (Summary value : values) {
			counter += value.getRequests();
			downloadSize += value.getDownloadSize();
		}
		temp = new Summary(counter, downloadSize);
		context.write(key, temp);
	}
	}
	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("System error 2304");
			System.exit(-1);
		}

		Job job = new Job();
		job.setJarByClass(RequestSummary.class);
		job.setJobName("Request Summary Counter");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(RequestSummaryMapper.class);
		job.setReducerClass(RequestSummaryReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Summary.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
