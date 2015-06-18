/**
 * 
 */
package edu.cornell.cs.sa;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.*;

/**
 * @author sonic
 *
 */
public class VectorClock implements ILogicalClock,Serializable
{
	private static final long serialVersionUID = -8748121323197839581L;
	public Map<Integer,Long> vc;
	public int pid;
	
	/**
	 * convert to byte array
	 * @return
	 * @throws IOException
	 */
	public byte[] toByteArrayNoPid() throws IOException{
	  ByteArrayOutputStream baos = new ByteArrayOutputStream();
	  ObjectOutputStream oos = new ObjectOutputStream(baos);
	  oos.writeObject(this.vc);
	  oos.flush();
	  return baos.toByteArray();
	}
	
	/**
	 * setFromByteArray
	 * @param vcObj
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	@SuppressWarnings("unchecked")
  public void fromByteArrayNoPid(byte vcObj[]) throws IOException, ClassNotFoundException{
	  ByteArrayInputStream bais = new ByteArrayInputStream(vcObj);
	  ObjectInputStream ois = new ObjectInputStream(bais);
	  this.vc = (Map<Integer,Long>)ois.readObject();
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
	}
	
	public VectorClock(VectorClock vco){
		vc = new HashMap<Integer,Long> (vco.vc);
		pid = vco.pid;
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
		return this;
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

		return this;
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
}
