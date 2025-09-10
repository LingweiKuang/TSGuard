#!/usr/bin/env bash

sudo systemctl start taosd
sudo systemctl start taosadapter
sudo systemctl start taoskeeper
sudo systemctl start taos-explorer
