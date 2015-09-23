public class JNICopy{
  static byte arr[]=new byte[1<<28];
  static{
    System.loadLibrary("JNICopy");
  }

  static native void javatoc(byte [] arr);
  static native void ctojava(byte [] arr);

  public static void main(String args[]){
    ctojava(arr);
    javatoc(arr);
  }
}
