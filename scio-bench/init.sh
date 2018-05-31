#!/bin/bash

git clone git@github.com:dcapwell/lightweight-java-profiler.git
cd lightweight-java-profiler
make
cp build-64/liblagent.so ../
cd ..

git clone git@github.com:brendangregg/FlameGraph.git
