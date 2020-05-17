#!/bin/sh

PASSWD=$1

./lobsters.py -p $PASSWD add_user -u user1
./lobsters.py -p $PASSWD add_user -u user2
./lobsters.py -p $PASSWD add_user -u user3

./lobsters.py -p $PASSWD add_story -u 1 -t "My story" -d "Test ..." -s qoaisf
./lobsters.py -p $PASSWD add_story -u 2 -t "You won't believe this" -d "lorem ipsum" -s towieu

./lobsters.py -p $PASSWD add_comment -u 2 -s 1 -c "interesting !"
./lobsters.py -p $PASSWD add_comment -u 1 -s 2 -c "lool"

./lobsters.py -p $PASSWD upvote_story -u 1 -s 2
./lobsters.py -p $PASSWD upvote_story -u 2 -s 1
./lobsters.py -p $PASSWD upvote_story -u 3 -s 1

./lobsters.py -p 123456 upvote_comment -u 2 -s 1 -c 1
./lobsters.py -p 123456 upvote_comment -u 1 -s 2 -c 2