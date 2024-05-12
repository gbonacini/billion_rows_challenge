Introduction:
=============

* I wrote this program after reading this article:
https://tspeterkim.github.io/posts/cuda-1brc <BR>
about this challenge:<BR> 
https://1brc.dev/#the-challenge <BR>

* This won't be direct candidate to the challenge because I think that different approaches could guarantee better performance, but a way to verify that the program in the first article can me improved. Infact my version use less then an half of GPU memory and run two second faster on my laptop pc ( not a server with H100 Nvidia GPU like the author of the article did).

* Also, the author of the artiche preprocessed the input file (my program doesn't require that, it use directly the challenge file without knowing any information about it): 
"So this is where I bend the rules of the original challenge a bit. I assume that a list of all cities is given along with the input file". 

Testing:
=========

Tested on:
* Ubuntu 22.04.4 LTS
* nvcc / CUDA 12.4

* A testing AWK script is provided in  ./test. It cat produce the aggregation for a single city as follow:
```shell
 cat measurements.txt | grep Zanzibar | ./check.awk 
MIN: -22.100000
AVG: 25.984645
MAX: 76.300000
```
* Note approximation error due to floats.

Dependencies:
=============

* CUDA 12.4

Installation and Use:
=====================

- compile the program as follow :
```shell
  make clean all
```
- install it (optional):
```shell
  sudo make install
```
- create 1 billion rows test file:
```shell
  make testfile
```
- run the program:
```shell
$ time ./process ./ measurements.txt
```

Performances:
=============

- This program can be improved in may ways, for example, implementing a custom map to replace STL version.

Test metrics
============

* coming soon.
