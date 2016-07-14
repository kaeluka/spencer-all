#!/usr/bin/env bash

function getNewestVersion() {
  git describe --abbrev=0 --tags
}
