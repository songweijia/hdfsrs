#!/usr/bin/python

from PIL import Image

'''
im = Image.open("horse.jpg")
cnt = 0
for x in [25,399,773,1147]:
  for y in [30,272,516]:
    box = [x,y,x+360,y+220]
    r = im.crop(box)
    r.save("data/horse%s.jpg" % str(cnt))
    cnt = cnt + 1
'''

of=open("data/horse.dat","w");

for i in range(0,11):
  r = Image.open("data/horse%s.jpg" % str(i))
  width = r.size[0]
  height = r.size[1]
  barr = bytearray(width*height);
  pix = r.load()
  
  for y in range(0,height):
    for x in range(0,width):
      barr[y*width+x] = pix[x,y]
  of.write(barr);

of.flush();
of.close();
