/**
 * 
 */
package edu.cornell.cs.sa;

import java.util.List;

/**
 * @author sonic
 * interface for the logical clock implementations
 */
public interface ILogicalClock {
	/**
	 * tick the logical clock
	 * @return
	 */
	public ILogicalClock tick();
	/**
	 * tick the logical clock on receiving a message.
	 * @param mlc - logical clock from the message
	 * @return
	 */
	public ILogicalClock tickOnRecv(ILogicalClock mlc);
	/**
	 * test if my clock happensBefore lc.
	 * @param lc
	 * @return 0 for unknown, 1 for true, -1 for false
	 */
//	int happensBefore(ILogicalClock lc);
	/**
	 * calculate a causality consistent clock from a set of clock
	 * @param pl is an array of clocks.
	 * @return a causally consistent logical clock.
	 */
	public ILogicalClock getCausallyConsistentClock(ILogicalClock[] clock);
}
