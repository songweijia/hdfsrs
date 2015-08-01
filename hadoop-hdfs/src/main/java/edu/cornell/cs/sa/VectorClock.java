/**
 * 
 */
package edu.cornell.cs.sa;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.VectorClockProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
/**
 * @author sonic
 *
 */
public class VectorClock implements ILogicalClock
{
	public Map<Integer,Long> vc;
	public int pid;
	
	/**
	 * convert to byte array
	 * @return
	 * @throws IOException
	 */
	public byte[] toByteArrayNoPid() throws IOException{
	  VectorClockProto vcp = PBHelper.convert(this);
	  return vcp.toByteArray();
/*	  
	  ByteArrayOutputStream baos = new ByteArrayOutputStream();
	  ObjectOutputStream oos = new ObjectOutputStream(baos);
	  synchronized(this){
	    oos.writeObject(this.vc);
	  }
	  oos.flush();
	  return baos.toByteArray();
*/
	}
	
	/**
	 * setFromByteArray
	 * @param vcObj
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	@SuppressWarnings("unchecked")
    synchronized public void fromByteArrayNoPid(byte vcObj[]) throws IOException, ClassNotFoundException{
	  this.vc = PBHelper.convert(VectorClockProto.parseFrom(vcObj)).vc;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof VectorClock) {
			VectorClock vco = (VectorClock) obj;
			Iterator<Integer> itr = vc.keySet().iterator();
			
			while(itr.hasNext()) {
			    int id = itr.next();
			    
			    if (vc.get(id)!=vco.vc.get(id))
			        return false;
            }
			return true;
		}
		return false;
	}
	
    public VectorClock(int pid){
      this.vc = new HashMap<Integer,Long> ();
      this.pid = pid;
      if(pid >= 0){
        this.vc.put(pid, 0l);
      }
    }
	
    public VectorClock(){
      this(-1);
    }
	
    public VectorClock(VectorClock vco){
      synchronized(vco){
        vc = new HashMap<Integer,Long> (vco.vc);
        pid = vco.pid;
      }
    }
	
	public long GetVectorClockValue(int rank){
	    return vc.get(rank);
	}

	/* (non-Javadoc)
	 * @see edu.cornell.cs.sa.ILogicalClock#tick()
	 */
	@Override
	synchronized public ILogicalClock tick() {
		if(pid>=0)//if pid < 0, we don't tick.
		{
          Long vcl = vc.get(pid);
          if(vcl==null) vcl = new Long(0);
          vc.put(pid, vcl+1);
		}
		return new VectorClock(this);
	}

	/* (non-Javadoc)
	 * @see edu.cornell.cs.sa.ILogicalClock#tickOnRecv(edu.cornell.cs.sa.ILogicalClock)
	 */
	@Override
	synchronized public ILogicalClock tickOnRecv(ILogicalClock mlc) {
		VectorClock mvc = (VectorClock) mlc;
		
		tick();
		for(int p : mvc.vc.keySet()){
			if(vc.get(p)==null || vc.get(p)<mvc.vc.get(p))
				vc.put(p, mvc.vc.get(p));
		}

        mvc.pid = pid;
        mvc.vc = new HashMap<Integer,Long> (vc);
		return mvc;
	}

	/* (non-Javadoc)
	 * @see edu.cornell.cs.sa.ILogicalClock#happensBefore(edu.cornell.cs.sa.ILogicalClock)
	 */
	/*
	@Override
	public int happensBefore(ILogicalClock mlc) {
		VectorClock mvc = (VectorClock) lc;
		int bRet = 0;
		for(int i=0;i<vc.length;i++){
			if(this.vc[i]>ovc.vc[i]){
				if(bRet==0||bRet==-1)bRet = -1;
				else return 0;
			}else if(this.vc[i]<ovc.vc[i]){
				if(bRet==0||bRet==1)bRet = 1;
				else return 0;
			}else{//==
				continue;
			}
		}
		return bRet;
	}*/

	@Override
	public ILogicalClock getCausallyConsistentClock (ILogicalClock[] lcs) {
	    if (lcs==null || lcs.length==0)
	        return null;
	        
//	    int nD = ((VectorClock)pl[0]).vc.length;
	    VectorClock vc1 = new VectorClock(-1);
	    
		for (int i = 0; i < lcs.length; i++) {
		    VectorClock vc2 = (VectorClock) lcs[i];
		    Iterator<Integer> itr1 = vc1.vc.keySet().iterator();
		    Iterator<Integer> itr2 = vc2.vc.keySet().iterator();
		    
		    while (itr1.hasNext()) {
		        int id = itr1.next();
		    
		        if ((vc2.vc.get(id) != null) && (vc2.vc.get(id) > vc1.vc.get(id)))
		            vc1.vc.put(id, vc2.vc.get(id));
		    }
		    while (itr2.hasNext()) {
		        int id = itr2.next();
		    
		        if (vc1.vc.get(id) == null)
		            vc1.vc.put(id, vc2.vc.get(id));
		    }
		}
		
		return vc1;
	}
	
	@Override
	public String toString(){
		StringBuffer sb = new StringBuffer("{");
		Iterator<Integer> itr = vc.keySet().iterator();
		
		while (itr.hasNext()) {
		    int id = itr.next();
			sb.append("(" + id + "," + vc.get(id) + ")");
			if (id == pid)
				sb.append('*');
			if (itr.hasNext())
				sb.append(',');
		}
		sb.append("}");
		return sb.toString();
	}
	
	static public String toString(byte[] byteArray) throws IOException, ClassNotFoundException{
	  VectorClock vc = new VectorClock(-1);
	  
	  vc.fromByteArrayNoPid(byteArray);
	  return vc.toString();
	}
}

