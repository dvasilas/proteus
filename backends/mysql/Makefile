.PHONY: all
all: lib_sys_exec.so

lib_sys_exec.so:
	gcc -Wall -I/usr/local/mysql/include/ -shared -o lib_sys_exec.so lib_sys_exec.c -fPIC