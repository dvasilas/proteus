#!/usr/bin/env python

import sys
import argparse

import mysql.connector
from mysql.connector import errorcode


class Table(object):
    def __init__(self, name, description, dumpfunc):
        self.name = name
        self.dumpfunc = dumpfunc


tables = {}
tables["users"] = ()
tables["stories"] = ()
tables["comments"] = ()
tables["votes"] = ()
tables["stories_votecount"] = ()
tables["comments_votecount"] = ()
tables["stories_with_votecount"] = ()
tables["comments_with_votecount"] = ()


class DB(object):
    def __init__(self, password):
        self.config = {
            "user": "user",
            "password": password,
            "host": "127.0.0.1",
            "port": "3307",
            "database": "proteus_lobsters_db",
        }
        self.cnx = mysql.connector.connect(**self.config)
        self.cursor = self.cnx.cursor()

        self.execute("USE {}".format(self.config["database"]))

        self.tables = [
            "users",
            "stories",
            "comments",
            "votes",
            "stories_votecount",
            "comments_votecount",
            "stories_with_votecount",
            "comments_with_votecount",
        ]

    def __del__(self):
        self.cursor.close()
        self.cnx.close()

    def execute(self, cmd, args=None):
        print(cmd, args)
        try:
            self.cursor.execute(cmd, args)
        except mysql.connector.Error as err:
            print("{} failed: {}".format(cmd, err))
            exit(1)

    def create_schema(self, args):
        self.create_database()

        for table_name in self.tables:
            table_description = tables[table_name]
            print("Creating table {}".format(table_name))
            self.execute(table_description)

    def dump(self, args):
        for table_name in self.tables:
            print(table_name)
            print("-----------")
            self.execute("SELECT * FROM `{}`".format(table_name))
            getattr(self, table_name)()
            print("===========")
            print()

    def get_homepage(self, args):
        self.execute(
            "SELECT * "
            " FROM stories_with_votecount "
            " ORDER BY `vote_count` DESC "
            " LIMIT 2 "
        )
        stories = self.cursor.fetchall()
        for story in stories:
            print("{}  score:{}\n{}".format(story[2], story[5], story[3]))

    def get_story(self, args):
        self.execute(
            "SELECT * FROM `stories_with_votecount` WHERE `short_id` = %s", (args.id,)
        )
        story = self.cursor.fetchall()

        self.execute(
            "SELECT `*` FROM `comments_with_votecount` WHERE `story_id` = %s ORDER BY `vote_count` DESC LIMIT 2 ",
            (story[0][0],),
        )

        print("{}  score:{}\n{}".format(story[0][2], story[0][5], story[0][3]))
        comments = self.cursor.fetchall()
        for comment in comments:
            print("|_ {}  score:{}\n".format(comment[3], comment[4]))

    def add_user(self, args):
        self.execute("INSERT INTO users (username) VALUES (%s)", (args.username,))
        self.cnx.commit()

    def add_story(self, args):
        self.execute(
            "INSERT INTO stories (user_id, title, description, short_id) VALUES (%s, %s, %s, %s)",
            (args.user_id, args.title, args.description, args.short_id),
        )
        self.cnx.commit()

    def add_comment(self, args):
        self.execute(
            "INSERT INTO comments (user_id, story_id, comment) VALUES (%s, %s, %s)",
            (args.user_id, args.story_id, args.comment),
        )
        self.cnx.commit()

    def upvote_story(self, args):
        self.execute(
            "INSERT INTO votes (user_id, story_id, vote) VALUES (%s, %s, 1)",
            (args.user_id, args.story_id),
        )
        self.cnx.commit()

    def downvote_story(self, args):
        self.execute(
            "INSERT INTO votes (user_id, story_id, vote) VALUES (%s, %s, -1)",
            (args.user_id, args.story_id),
        )
        self.cnx.commit()

    def upvote_comment(self, args):
        self.execute(
            "INSERT INTO votes (user_id, story_id, comment_id, vote) VALUES (%s, %s, %s, 1)",
            (args.user_id, args.story_id, args.comment_id),
        )
        self.cnx.commit()

    def downvote_comment(self, args):
        self.execute(
            "INSERT INTO votes (user_id, story_id, comment_id, vote) VALUES (%s, %s, %s, -1)",
            (args.user_id, args.story_id, args.comment_id),
        )
        self.cnx.commit()

    def users(self):
        for (user_id, username) in self.cursor:
            print("user_id: {}, username: {}".format(user_id, username))

    def stories(self):
        for (story_id, user_id, title, description, short_id, ts) in self.cursor:
            print(
                "story_id: {}, user_id: {}, title: {}, description: {}, short_id: {}, ts: {}".format(
                    story_id, user_id, title, description, short_id, ts
                )
            )

    def comments(self):
        for (comment_id, story_id, user_id, comment, ts) in self.cursor:
            print(
                "comment_id: {}, story_id: {}, user_id: {}, comment: {}, ts: {}".format(
                    comment_id, story_id, user_id, comment, ts
                )
            )

    def votes(self):
        for (vote_id, user_id, story_id, comment_id, vote, ts) in self.cursor:
            print(
                "vote_id: {}, user_id: {}, story_id: {}, comment_id: {}, vote: {}, ts: {}".format(
                    vote_id, user_id, story_id, comment_id, vote, ts
                )
            )

    def stories_votecount(self):
        for (story_id, vote_count, ts) in self.cursor:
            print("story_id: {}, vote_count: {}, ts: {}".format(story_id, vote_count, ts))

    def comments_votecount(self):
        for (story_id, comment_id, vote_count, ts) in self.cursor:
            print(
                "story_id: {}, comment_id: {}, vote_count: {}, ts: {}".format(
                    story_id, comment_id, vote_count, ts
                )
            )

    def stories_with_votecount(self):
        for (
            story_id,
            user_id,
            title,
            description,
            short_id,
            vote_count,
            ts,
        ) in self.cursor:
            print(
                "story_id: {}, user_id: {}, title: {}, description: {}, short_id: {}, vote_count: {}, ts: {}".format(
                    story_id, user_id, title, description, short_id, vote_count, ts
                )
            )

    def comments_with_votecount(self):
        for (comment_id, story_id, user_id, comment, vote_count, ts) in self.cursor:
            print(
                "comment_id: {}, story_id: {}, user_id: {}, comment: {}, vote_count: {}, ts: {}".format(
                    comment_id, story_id, user_id, comment, vote_count, ts
                )
            )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--password", "-p", required=True, help="the MySQL password")

    subpraser = parser.add_subparsers(dest="command")
    dump = subpraser.add_parser("dump", help="Dump database")
    get_homepage = subpraser.add_parser(
        "get_homepage", help="Get the Lobste.rs homepage"
    )

    get_story = subpraser.add_parser("get_story", help="Get a story by short_id")
    get_story.add_argument(
        "--id", required=True, help="The short_id for the story to get"
    )

    add_user = subpraser.add_parser("add_user", help="Create a new user")
    add_user.add_argument(
        "--username", "-u", required=True, help="The username of the user to add"
    )

    add_story = subpraser.add_parser("add_story", help="Post a new story")
    add_story.add_argument("--user_id", "-u", required=True)
    add_story.add_argument("--title", "-t", required=True)
    add_story.add_argument("--description", "-d", required=True)
    add_story.add_argument("--short_id", "-s", required=True)

    add_comment = subpraser.add_parser("add_comment", help="Post a new comment")
    add_comment.add_argument("--user_id", "-u", required=True)
    add_comment.add_argument("--story_id", "-s", required=True)
    add_comment.add_argument("--comment", "-c", required=True)

    upvote_story = subpraser.add_parser("upvote_story", help="Upvote a story")
    upvote_story.add_argument("--user_id", "-u", required=True)
    upvote_story.add_argument("--story_id", "-s", required=True)

    downvote_story = subpraser.add_parser("downvote_story", help="Downvote a story")
    downvote_story.add_argument("--user_id", "-u", required=True)
    downvote_story.add_argument("--story_id", "-s", required=True)

    upvote_comment = subpraser.add_parser("upvote_comment", help="Upvote a comment")
    upvote_comment.add_argument("--user_id", "-u", required=True)
    upvote_comment.add_argument("--story_id", "-s", required=True)
    upvote_comment.add_argument("--comment_id", "-c", required=True)

    downvote_comment = subpraser.add_parser("downvote_comment", help="Upvote a comment")
    downvote_comment.add_argument("--user_id", "-u", required=True)
    downvote_comment.add_argument("--story_id", "-s", required=True)
    downvote_comment.add_argument("--comment_id", "-c", required=True)

    args = parser.parse_args()

    db = DB(args.password)

    if args.command:
        pass
        getattr(db, args.command)(args)

    del db


if __name__ == "__main__":
    main()
