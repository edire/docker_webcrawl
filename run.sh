#!/bin/sh

git clone --depth 1 $GIT_REPO app
cd app
python app.py