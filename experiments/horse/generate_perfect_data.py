#!/usr/bin/python

from PIL import Image


millsec=0;

for i in range(0,11):
  r = Image.open("data/horse%s.jpg" % str(i))
  width = r.size[0]
  height = r.size[1]
  barr = bytearray(width*height);
  pix = r.load()
  of=open("data/perfect/%s.dat" % '{:04d}'.format(millsec),"w");
  for y in range(0,height):
    for x in range(0,width):
      barr[y*width+x] = pix[x,y]
  of.write(barr);
  of.flush();
  of.close();
  millsec = millsec + 40
