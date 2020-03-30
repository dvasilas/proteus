import sys

f= open("test_trigger.txt","a")
f.write("Triggered: id {}, data {}\n".format(sys.argv[1], sys.argv[2]))
f.close()