package edu.cornell.cs.sa;

import edu.cornell.cs.blog.*;

public class HybridLogicalClock implements Comparable<HybridLogicalClock>
{
  public long r;
  public long c;
  
  public HybridLogicalClock() {
	this.r = 0;
	this.c = 0;
  }
  
  public HybridLogicalClock(HybridLogicalClock hlc) {
    this.r = hlc.r;
    this.c = hlc.c;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof HybridLogicalClock))
      return false;
    
    HybridLogicalClock hlc = (HybridLogicalClock) obj;
    
    return ((hlc.r == this.r) && (hlc.c == this.c));
  }
  
  @Override
  public int compareTo(HybridLogicalClock hlc) {
    if (r == hlc.r)
      return Long.compare(c, hlc.c);
    return Long.compare(r, hlc.r);
  }
  
  synchronized public void tick() {
    long previous_r = r;
    long rtc = JNIBlog.readLocalRTC();
    
    r = Math.max(previous_r, rtc);
    if (r == previous_r)
      c++;
    else
      c = 0;
  }
  
  synchronized public HybridLogicalClock tickCopy(){
    tick();
    return new HybridLogicalClock(this);
  }
  
  synchronized public void tickOnRecv(HybridLogicalClock mhlc) {
    long previous_r = r;
    long rtc = JNIBlog.readLocalRTC();
    
    r = Math.max(previous_r, Math.max(mhlc.r, rtc));
    if ((r == previous_r) && (r == mhlc.r))
      c = Math.max(c, mhlc.c) + 1;
    else if (r == previous_r)
      c++;
    else if (r == mhlc.r)
      c = mhlc.c + 1;
    else
      c = 0;
    mhlc.r = r;
    mhlc.c = c;
  }
  
  synchronized public HybridLogicalClock tickOnRecvCopy(HybridLogicalClock mhlc){
    tickOnRecv(mhlc);
    return new HybridLogicalClock(this);
  }
  
  synchronized public void tickOnRecvWriteBack(HybridLogicalClock mhlc){
    tickOnRecv(mhlc);
    mhlc.c = this.c;
    mhlc.r = this.r;
  }
  
  synchronized public void mockTick(long rtc) {
    long previous_r = r;
    
    r = Math.max(previous_r, rtc);
    if (r != previous_r)
      c = 0;
  }

  @Override
  public String toString() {
    Long r = new Long(this.r);
    Long c = new Long(this.c);
  
    return "(" + r.toString() + "," + c.toString() + ")";
  }
}
