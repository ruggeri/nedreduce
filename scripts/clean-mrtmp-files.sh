#!/bin/bash

gfind . -regextype posix-extended -regex ".*/(mrtmp.wcseq-|824-mrinput-).*" | parallel "rm {}"
