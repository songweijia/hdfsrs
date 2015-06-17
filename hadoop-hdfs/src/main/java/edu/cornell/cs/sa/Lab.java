package edu.cornell.cs.sa;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Vector;

import org.junit.Test;

public class Lab{
	
	/**
	 * CASE ONE:The empty Test:
	 * P0: (0*,0,0)
	 * P1: (0,0*,0)
	 * P2: (0,0,0*)
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testCase1(){
/*		List [] pl = {new Vector<ILogicalClock>(), //p0
				new Vector<ILogicalClock>(), //p1
				new Vector<ILogicalClock>(), //p2
				};
		initEvents(pl[0],0,"000");
		initEvents(pl[1],1,"000");
		initEvents(pl[2],2,"000");
		VectorClock vce = vc("000",-1);
		VectorClock vcs = (VectorClock)vce.getCausallyConsistentClock(pl);
		assertEquals(vcs,vce);
*/	}
	
	/**
	 * Test Case TWO: Simple with one advance
	 * P0: (1*,1)
	 * P1: (0,0*)->(0,1*)
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testCase2(){
/*		List [] pl = {new Vector<ILogicalClock>(), //p0
				new Vector<ILogicalClock>(), //p1
		};
		initEvents(pl[0],0,"11");
		initEvents(pl[1],0,"00","01");
		VectorClock vce = vc("11",-1);
		VectorClock vcs = (VectorClock)vce.getCausallyConsistentClock(pl);
		assertEquals(vcs,vce);
*/	}
	
	
	VectorClock vc(String c,int pt){
/*		VectorClock v = new VectorClock(c.length(),pt);
		byte cb[] = c.getBytes();
		for(int i=0;i<c.length();i++)
			v.vc[i]=cb[i]-'0';
*/
	  return null;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	void initEvents(List l, int pt, String ... vcs){
		for(int i=0;i<vcs.length;i++)
			l.add(vc(vcs[i],pt));
	}
	
	/**
	 * p0:[0]0000-[1]0000-[2]0000-[3]0000-[4]0000
	 * p1 1[1]000-1[2]000-1[3]000-1[4]000-1[5]000
	 * p2 30[1]00-30[2]00-30[3]00-33[4]20-33[5]20
	 * p3 333[1]0-333[2]0-333[3]0-333[4]0-333[5]0
	 * p4 3352[1]-3352[2]-3352[3]-3352[4]-3352[5]
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testCase3(){
/*		List [] pl = {new Vector<ILogicalClock>(), //p0
				new Vector<ILogicalClock>(), //p1
				new Vector<ILogicalClock>(), //p2
				new Vector<ILogicalClock>(), //p3
				new Vector<ILogicalClock>(), //p4
		};
		initEvents(pl[0],0,"00000","10000","20000","30000","40000");
		initEvents(pl[1],1,"11000","12000","13000","14000","15000");
		initEvents(pl[2],2,"30100","30200","30300","33420","33520");
		initEvents(pl[3],3,"33310","33320","33330","33340","33350");
		initEvents(pl[4],4,"33521","33522","33523","33524","33525");
		VectorClock vce = vc("33521",-1);
		VectorClock vcs = (VectorClock)vce.getCausallyConsistentClock(pl);
		assertEquals(vcs,vce);
*/	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
	}

}
