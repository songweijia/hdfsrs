#!/usr/bin/python

import Image

im = Image.open("horse.jpg")
cnt = 0
for x in [25,399,773,1147]:
  for y in [30,272,516]:
    box = [x,y,x+360,y+220]
    r = im.crop(box)
    r.save("data/horse%s.jpg" % str(cnt))
    cnt = cnt + 1
