#include <cstdlib>
#include <cassert>
#include "util.hpp"

#define ALIGN(x, a)	(((x) + (a)) & ~((a) - 1))

void *aligned_malloc(size_t size, size_t alignment)
{
	//unsigned int len = (size + sizeof(void*) + alignment) & ~alignment;
	unsigned int len = ALIGN(size + sizeof(void*), alignment);
	void *optr = malloc(len);
	void *aligned_ptr = nullptr;
	static auto total_allocated = 0u;

	if (!optr) {
		printf("%s: malloc returned %p for request len %u bytes (size = %zu | alignment = %zu) "
			" failed after allocating %u bytes\n",
					__func__, optr, len, size, alignment, total_allocated);
		return nullptr;
	}

	total_allocated += len;
	// handle 2 cases
	// 1) returned pointer is already aligned
	// 2) returned pointer is not aligned
	//aligned_ptr = (void*)((unsigned long long)(optr) + sizeof(void*) + alignment) & ~alignment;
	aligned_ptr = (void*)ALIGN((unsigned long long)optr + sizeof(void*), alignment);

	*((unsigned long long*)aligned_ptr - 1) = (unsigned long long)(optr);

	//printf("%s, aligned_ptr %p | alignment 0x%zx\n", __func__, aligned_ptr, alignment);
	assert( ((unsigned long long) aligned_ptr & ~(alignment - 1)) == (unsigned long long) aligned_ptr);
	return aligned_ptr;
}


void aligned_free(void *aligned_ptr)
{
	void *optr = (void*)(*((unsigned long long*)aligned_ptr - 1));
	printf("%s, aligned_free %p\n", optr);
	free(optr);
}
