#!/bin/sh

git clone -b $GIT_BRANCH --depth 1 $GIT_REPO git_repo
cp -r /templates /git_repo/app/templates
cd /git_repo/app
python app.py