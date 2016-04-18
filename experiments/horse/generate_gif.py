#!/usr/bin/python

import Image
import sys, os
from glob import glob
from PIL import Image, ImageSequence
from images2gif import writeGif

def main(data_path,width,height,duration):
  files_list = glob(os.path.join(data_path, '*.dat'))
  last = -1 # last timestamp
  images = []
  for fn in sorted(files_list):
    now = int(fn.split("/")[-1].split(".")[0])
    #insert it into the gif.
    f = open(fn,'r')
    img = Image.new('L',(width,height),"white")
    pix = img.load()
    for y in range(0,height):
      for x in range(0,width):
        pix[x,y] = ord(f.read(1))
    f.close()
    images.append(img)
  writeGif("out.gif", images, duration=duration);

if __name__ == '__main__':
  if len(sys.argv) != 5:
    print "Usage: %s <data_path> <width> <height> <duration in sec>" % str(sys.argv[0])
  else:
    main(sys.argv[1],int(sys.argv[2]),int(sys.argv[3]),float(sys.argv[4]))

'''
millsec=0;

for i in range(0,11):
  r = Image.open("data/horse%s.jpg" % str(i))
  width = r.size[0]
  height = r.size[1]
  barr = bytearray(width*height);
  pix = r.load()
  of=open("data/perfect/frame_%s.dat" % '{:04d}'.format(millsec),"w");
  for y in range(0,height):
    for x in range(0,width):
      barr[y*width+x] = pix[x,y]
  of.write(barr);
  of.flush();
  of.close();
  millsec = millsec + 40
'''
