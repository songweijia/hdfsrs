/**
 * 
 */
package edu.cornell.cs.sa;

import java.util.List;

/**
 * @author sonic
 *
 */
public class LamportClock implements ILogicalClock {

	long c = 0;
	/* (non-Javadoc)
	 * @see edu.cornell.cs.sa.ILogicalClock#tick()
	 */
	@Override
	synchronized public ILogicalClock tick() {
		c++;
		return this;
	}

	/* (non-Javadoc)
	 * @see edu.cornell.cs.sa.ILogicalClock#tickOnRecv(edu.cornell.cs.sa.ILogicalClock)
	 */
	@Override
	public ILogicalClock tickOnRecv(ILogicalClock mlc) {
		c++;
		LamportClock mc = (LamportClock)mlc;
		if(c<mc.c+1)c = mc.c + 1;
		return this;
	}

	/* (non-Javadoc)
	 * @see edu.cornell.cs.sa.ILogicalClock#happensBefore(edu.cornell.cs.sa.ILogicalClock)
	 */
	public int happensBefore(ILogicalClock olc) {
		LamportClock oc = (LamportClock)olc;
		int ret = 0;
		if(c<oc.c)ret = 1;
		else if(c>oc.c)ret = -1;
		return ret;
	}

	/* (non-Javadoc)
	 * @see edu.cornell.cs.sa.ILogicalClock#getCausallyConsistentClock(java.util.List)
	 */
	@Override
	public ILogicalClock getCausallyConsistentClock(ILogicalClock[] pl) {
		//TODO: list of clocks
		LamportClock ccc = new LamportClock();
		for(int i=0;i<pl.length;i++){
			LamportClock lc = (LamportClock)pl[i];
			if(ccc.happensBefore(lc)==1)ccc.c = lc.c;
		}
		return ccc;
	}
}
