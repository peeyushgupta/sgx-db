
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

/* Open a file descriptor to store the table */
int ocall_open_file(const char *name) {
	int fd; 

	fd = open(name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
	return fd; 
};

int ocall_write_file(int fd, void* buf, unsigned long size) {
	return write(fd, buf, size);
};

int ocall_read_file(int fd, void* buf, unsigned long size) {
	return read(fd, buf, size);
};

int ocall_seek(int fd, unsigned long offset) {
	return lseek(fd, offset, SEEK_SET);
};


