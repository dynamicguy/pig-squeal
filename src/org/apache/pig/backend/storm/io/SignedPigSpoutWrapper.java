package org.apache.pig.backend.storm.io;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.Expression;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.storm.oper.TriMakePigTuples;
import org.apache.pig.builtin.PigStreaming;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.mortbay.util.ajax.JSON;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import backtype.storm.generated.StreamInfo;
import backtype.storm.spout.ISpout;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsGetter;
import backtype.storm.tuple.Values;

public class SignedPigSpoutWrapper extends SpoutWrapper {
	
	public SignedPigSpoutWrapper(String spoutClass) {
		super(spoutClass, null);
	}
	
	public SignedPigSpoutWrapper(String spoutClass, String jsonArgs) {
		super(spoutClass, jsonArgs, null);
	}
	
	public SignedPigSpoutWrapper(String spoutClass, String jsonArgs, String parallelismHint) {
		super(spoutClass, jsonArgs, parallelismHint);
	}

	@Override
	public ResourceSchema getSchema(String location, Job job)
			throws IOException {
		return null;
	}
	
	public Class<? extends BaseFunction> getTupleConverter() {
		return MakePigTuples.class;
	}
	
	static public class MakePigTuples extends BaseFunction {
		private TupleFactory tf;
		private PigStreaming ps;
		
		@Override
		public void prepare(java.util.Map conf, TridentOperationContext context) {
			 tf = TupleFactory.getInstance();
			 ps = new PigStreaming();
		}
			
		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			byte[] buf = (byte[]) tuple.get(0);
			
			Tuple t;
			Integer sign;
			try {
				t = ps.deserialize(buf);
//				System.err.println("SignedPigSpout.execute: " + t);
				List<Object> lst = t.getAll();
				DataByteArray dba = (DataByteArray) lst.remove(lst.size() - 1);
				sign = Integer.parseInt(dba.toString().replace("\n", ""));
				t = tf.newTupleNoCopy(lst);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			
			collector.emit(new Values(null, new NullableTuple(t), sign));
		}

	}
}
