
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <time.hpp>

#include "enclave_u.h"

#define IO_VERBOSE 0

void ocall_null_ocall(void) {

	return;
};

unsigned long long ocall_rdtsc(void) {

	return RDTSC();
};



void ocall_print_string(const char *str)
{
	/* Proxy/Bridge will check the length and null-terminate 
	* the input string to prevent buffer overflow. 
	*/
	printf("%s", str);
	return;
};

void *ocall_alloc_io_buf(unsigned long size) {
	return malloc(size);
}; 

void ocall_free_io_buf(void *buf) {
	free(buf);
	return; 
}; 

/* Open a file descriptor to store the table */
int ocall_open_file(const char *name) {
	int fd; 

	fd = open(name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
	DBG_ON(IO_VERBOSE, "open file, fd:%d\n", fd); 
	return fd; 
};

int ocall_write_file(int fd, void* buf, unsigned long size) {
	DBG_ON(IO_VERBOSE, "write file fd:%d, buf:%p, size:%lu\n", fd, buf, size);
	return write(fd, buf, size);
};

int ocall_read_file(int fd, void* buf, unsigned long size) {
	DBG_ON(IO_VERBOSE, "read file fd:%d, buf:%p, size:%lu\n", fd, buf, size);
	return read(fd, buf, size);
};

int ocall_seek(int fd, unsigned long offset) {
	return lseek(fd, offset, SEEK_SET);
};

int ocall_close_file(int fd) {
	DBG_ON(IO_VERBOSE, "close file fd:%d\n", fd);
	return close(fd);
};

int ocall_rm_file(const char *name) {
	DBG_ON(IO_VERBOSE, "rm file:%s\n", name);
	return unlink(name);
};

