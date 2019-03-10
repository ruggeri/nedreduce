#!/bin/bash

gfind . -regextype posix-extended -regex ".*/(mrtmp\..*-|824-mrinput-).*" | parallel "rm {}"
