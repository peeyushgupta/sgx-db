#pragma once 

#if defined(NO_SGX)

extern "C" {
	void ocall_print_string(const char *str);
}


/* File operations to read/write tables from disk */
int ocall_open_file(const char *name); 

void *ocall_alloc_io_buf(unsigned long size); 
void ocall_free_io_buf(void *buf); 

int ocall_write_file(int fd, void* buf, unsigned long size); 
int ocall_read_file(int fd, void* buf, unsigned long size);
int ocall_seek(int fd, unsigned long offset);

#endif
