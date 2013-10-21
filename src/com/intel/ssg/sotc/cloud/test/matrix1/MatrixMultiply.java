package com.intel.ssg.sotc.cloud.test.matrix1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MatrixMultiply {

	public static class Job1Key implements WritableComparable {
		private int matrixSeq;
		private int i;

		public void write(DataOutput out) throws IOException {
			out.writeInt(matrixSeq);
			out.writeInt(i);

		}

		public void readFields(DataInput in) throws IOException {
			matrixSeq = in.readInt();
			i = in.readInt();

		}

		public int compareTo(Object other) {
			Job1Key o = (Job1Key) other;
			if (this.i < o.i) {
				return -1;
			} else if (this.i > o.i) {
				return 1;
			} else {
				if (this.matrixSeq < o.matrixSeq) {
					return -1;
				} else if (this.matrixSeq > o.matrixSeq) {
					return 1;
				} else {
					return 0;
				}
			}
		}

		public int getMatrixSeq() {
			return matrixSeq;
		}

		public void setMatrixSeq(int matrixSeq) {
			this.matrixSeq = matrixSeq;
		}

		public int getI() {
			return i;
		}

		public void setI(int i) {
			this.i = i;
		}

		@Override
		public String toString() {
			return matrixSeq + "," + i;
		}

	}

	public static class Job1Value implements WritableComparable, Cloneable {
		private int i;
		private int j;
		private int value;

		public void write(DataOutput out) throws IOException {
			out.writeInt(i);
			out.writeInt(j);
			out.writeInt(value);
		}

		public void readFields(DataInput in) throws IOException {
			i = in.readInt();
			j = in.readInt();
			value = in.readInt();
		}

		public int compareTo(Object other) {
			Job1Value o = (Job1Value) other;
			if (this.i < o.i) {
				return -1;
			} else if (this.i > o.i) {
				return 1;
			}
			return 0;
		}

		public int getI() {
			return i;
		}

		public void setI(int i) {
			this.i = i;
		}

		public int getJ() {
			return j;
		}

		public void setJ(int j) {
			this.j = j;
		}

		public int getValue() {
			return value;
		}

		public void setValue(int value) {
			this.value = value;
		}

		@Override
		public String toString() {
			return i + "," + j + "," + value;
		}

		@Override
		public Job1Value clone() throws CloneNotSupportedException{
			return (Job1Value) super.clone();
		}

	}

	private static class Job1Partitioner extends
			Partitioner<Job1Key, Job1Value> {
		public int getPartition(Job1Key key, Job1Value value, int numPartitions) {
			return key.getI() % numPartitions;
		}
	}

	public static class Job1Mapper extends
			Mapper<Object, Text, Job1Key, Job1Value> {
		private Path path;
		private boolean isLeft;
		private Job1Value outValue = new Job1Value();
		private Job1Key outKey = new Job1Key();

		public void setup(Context context) {
			FileSplit split = (FileSplit) context.getInputSplit();
			path = split.getPath();
			isLeft = path.toString().indexOf("a.txt") != -1;
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String i = key.toString();
			String[] v = value.toString().split(",");
			for (int cr = 0; cr < v.length; cr++) {
				if (isLeft) {
					outKey.setMatrixSeq(0);
					outKey.setI(cr);

				} else {
					outKey.setMatrixSeq(1);
					outKey.setI(Integer.valueOf(i) / 6);
				}
				outValue.setI(Integer.valueOf(i) / 6);
				outValue.setJ(cr);
				outValue.setValue(Integer.valueOf(v[cr]));
				context.write(outKey, outValue);
			}
		}
	}

	public static class MultiplyReducer extends
			Reducer<Job1Key, Job1Value, Job1Key, Job1Value> {
		private List<Job1Value> leftList = new ArrayList<Job1Value>();
		private Job1Value outValue = new Job1Value();

		public void reduce(Job1Key key, Iterable<Job1Value> values,
				Context context) throws IOException, InterruptedException {
			if (key.getMatrixSeq() == 0) {
				leftList.clear();
				for (Job1Value val : values) {
					try {
						leftList.add(val.clone());
					} catch (CloneNotSupportedException e) {
						e.printStackTrace();
					}
				}
			} else {
				for (Job1Value right : values) {
					for (Job1Value left : leftList) {
						outValue.setI(left.getI());
						outValue.setJ(right.getJ());
						outValue.setValue(left.getValue() * right.getValue());
						context.write(null, outValue);
					}
				}
			}
		}
	}

	public static class Job2Mapper extends
			Mapper<Object, Text, Text, IntWritable> {

		Text outKey = new Text();
		IntWritable outValue = new IntWritable();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] v = value.toString().split(",");
			outKey.set(v[0] + "," + v[1]);
			outValue.set(Integer.valueOf(v[2]));
			context.write(outKey, outValue);
		}
	}

	public static class SumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable v : values) {
				sum += v.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 1) {
			System.err.println("Usage: matrix path");
			System.exit(2);
		}
		Job job1 = new Job(conf, "matrix multiply job 1");
		job1.setJarByClass(MatrixMultiply.class);
		job1.setMapperClass(Job1Mapper.class);
		job1.setPartitionerClass(Job1Partitioner.class);
		job1.setReducerClass(MultiplyReducer.class);
		job1.setOutputKeyClass(Job1Key.class);
		job1.setOutputValueClass(Job1Value.class);
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0] + "/A/a.txt"));
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0] + "/B/b.txt"));
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[0] + "/tmp"));
		job1.waitForCompletion(true);

		Job job2 = new Job(conf, "matrix multiply job 2");
		job2.setJarByClass(MatrixMultiply.class);
		job2.setMapperClass(Job2Mapper.class);
		job2.setReducerClass(SumReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job2, new Path(otherArgs[0] + "/tmp"));
		FileOutputFormat
				.setOutputPath(job2, new Path(otherArgs[0] + "/result"));
		job2.waitForCompletion(true);
	}
}
