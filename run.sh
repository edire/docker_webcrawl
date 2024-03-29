#!/bin/sh

git clone --depth 1 $GIT_REPO git_repo
cd git_repo/app/
python app.py