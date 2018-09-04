#pragma once 

#if defined(NO_SGX)

extern "C" {
	void ocall_print_string(const char *str);
}


/* File operations to read/write tables from disk */
int ocall_open_file(const char *name); 
int ocall_write_file(int fd, void* buf, unsigned long size); 
int ocall_read_file(int fd, void* buf, unsigned long size);
int ocall_seek(int fd, unsigned long offset);

#endif
