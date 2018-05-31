#!/bin/sh

DURATION=300

sbt -Dscio.version=0.5.4 -Dbeam.version=2.4.0 pack

micro_bench() {
    NAME=$1
    echo "Running $NAME"
    time java -agentpath:./liblagent.so \
        -Xms8g -Xmx8g -cp "target/pack/lib/*" com.spotify.MicroBench \
        --duration=$DURATION \
        --name=$NAME

    ./FlameGraph/stackcollapse-ljp.awk < traces.txt | ./FlameGraph/flamegraph.pl > $NAME.svg
}

SRC=src/main/scala/com/spotify/MicroBench.scala

for n in $(grep "^class .*MicroBench" $SRC | awk '{print $2}'); do
    micro_bench $n
done
