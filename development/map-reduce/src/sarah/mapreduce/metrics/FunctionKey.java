package sarah.mapreduce.metrics;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.BinaryComparable;

/*
 * FunctionKeys hold:
 * 		* the actual key returned by the mapper, 
 * 		* the function name, 
 * 		* the size of the record  
 * 
 * Keys are sorted on all three fields.
 * Keys are partitioned only by the function name -- one reducer per function.
 * A single reduce call processes all of the keys that match the function, the key value and the record size.
 */

@SuppressWarnings("rawtypes")
public class FunctionKey extends BinaryComparable implements WritableComparable<BinaryComparable>  {
	private String functionName;
	private WritableComparable functionKey;
	private long recordSize=0;
	private byte[] bytes = null;
	private static FunctionKey singletonKey= new FunctionKey();
	
	public FunctionKey() {
		clear();
	}
	
	void clear() {
		functionName = "";
		functionKey = null;
		bytes = null;
		recordSize = 0;
	}
	// This returns a key that is usable to write to the MapContext 
	public static WritableComparable getSingletonFunctionKey(String userFunctionName,WritableComparable key, long recordSize) {
		singletonKey.functionName = userFunctionName;
		singletonKey.functionKey=key;
		singletonKey.recordSize=recordSize;
		return singletonKey;
	}	
	
	public void setTo(FunctionKey otherKey) throws IOException {
		// Due to the Haoop pattern of reusing key objects, the otherKey needs to be copied.
		// The way to do this is to externalize it into a byte stream and internalize it back.
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(baos);
		otherKey.write(dos);
		
		bytes = baos.toByteArray();
		ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
		DataInputStream dis = new DataInputStream(bais);
		this.readFields(dis,false);
	}
	
	public boolean isSet() {
		return bytes!=null;
	}
	

	
	@Override
	public void readFields(DataInput in) throws IOException {
		readFields(in, true);
	}
	
	private void readFields(DataInput in, boolean setToSelf) throws IOException {
		functionName = in.readUTF();
		recordSize = in.readLong();
		try {
			Class keyClass = Class.forName(in.readUTF());
			functionKey = (WritableComparable) keyClass.newInstance();
			functionKey.readFields(in);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// In order to populate bytes, we need to set it to itself.  It's a lot of copying but
		if (setToSelf) setTo(this);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(functionName);
		out.writeLong(recordSize);
		out.writeUTF(functionKey.getClass().getName());
		functionKey.write(out);
	}

	
	@SuppressWarnings("unchecked")
	@Override
	public int compareTo(BinaryComparable otherKey) {
		int compareNames = functionName.compareTo(((FunctionKey)otherKey).functionName);
		if (compareNames != 0) {
			return compareNames;
		}
		int compareFunctions = functionKey.compareTo(((FunctionKey)otherKey).functionKey);
		if (compareFunctions!=0) {
			return compareFunctions;
		}
		long difference = (this.recordSize - ((FunctionKey)otherKey).recordSize);
		if (difference==0) return 0;
		if (difference>0) return 1;
		return -1;
	}
	
	@Override
	public int hashCode() {
		return functionName.hashCode() + functionKey.hashCode();
	}
	
	@Override
	public byte[] getBytes() {
		return bytes;
	}
	@Override
	public int getLength() {
		return bytes.length;
	}
	@Override
	public String toString() {
		if (!isSet()) return "null";
		return functionName+":"+functionKey.toString();
	}
	
	public String getFunctionName() {
		return functionName;
	}
	
	public WritableComparable getKey() {
		return functionKey;
	}

    public long getRecordSize() {
    	return recordSize;
    }

	public boolean differentKey(FunctionKey key) {
		// TODO Auto-generated method stub
		return !(functionName.equals(key.functionName) && functionKey.equals(key.functionKey));
	}




}
