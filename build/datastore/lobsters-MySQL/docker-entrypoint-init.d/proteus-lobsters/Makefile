.PHONY: all
all: lib_sys_exec.so

lib_sys_exec.so:
	gcc -Wall -I/usr/include/mysql -shared -o lib_sys_exec.so lib_sys_exec.c -fPIC