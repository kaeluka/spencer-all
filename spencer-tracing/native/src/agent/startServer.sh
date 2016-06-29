#!/bin/sh

function startServer() {
    # Start the transformation server:
    # (cd ../transformer/bin; java -Xbootclasspath/p:.:../lib/asm-all-5.0.3.jar:../lib/awio.jar transformer.TransformerServer ) &
    (cd ../transformer; make runServer) &
    #rm -f "/Volumes/My Book/prototracefile.log"
    # Sleep for a bit so the server is started up:
    sleep 3
}

startServer
