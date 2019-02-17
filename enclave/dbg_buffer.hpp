#ifndef _DBG_BUFFER_HPP
#define _DBG_BUFFER_HPP

struct dbg_buffer {
	char **buffers;
	unsigned int num_buffers;
	unsigned int current;

	explicit dbg_buffer(unsigned int bufs) :
		num_buffers(bufs), current(0) {

		buffers = new char*[bufs];
		for (auto i = 0u; i < num_buffers; i++)
			buffers[i] = new char[BUFSIZ];
	}

	~dbg_buffer() {
		for (auto i = 0u; i < num_buffers; i++)
			delete[] buffers[i];

		delete[] buffers;
	}

	void insert(const char *fmt, ...) {
		va_list args;

		current ++;

		if (current >= num_buffers) {
			ERR("buffer is full\n");
			return; 
		} 
		
		va_start(args, fmt);
		_vsprintf_s(buffers[current], BUFSIZ, fmt, args);
		va_end(args);
	}

	void flush(void) {
		for (auto i = 0u; i < num_buffers; i++)
			ocall_print_string(buffers[i]);
	}
};

#endif // _DBG_BUFFER_HPP
