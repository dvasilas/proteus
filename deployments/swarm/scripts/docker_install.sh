#!/bin/bash
ssh -i $1 $2@$3 'bash -s' < install.sh
