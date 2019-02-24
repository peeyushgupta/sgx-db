#pragma once

#include "spinlock.hpp"

//#define DATA_BLOCK_SIZE (1 << 12) /* 4 KB */
#define DATA_BLOCK_SIZE (1 << 16) /* 64 KB */

//#define DATA_BLOCK_SIZE (1 << 19) /* 512 KB */

//#define DATA_BLOCK_SIZE (1 << 20) /* 1 MB  */
//#define DATA_BLOCK_SIZE (1*(1 << 20)) /* X MB  */

#define DATA_BLKS_PER_DB 512 /* data blocks per DB */
//#define DATA_BLKS_PER_DB 7 /* data blocks per DB */



typedef struct data_block {
  int flags;
  struct table *table;
  unsigned long blk_num;
  volatile unsigned int refcnt;
  struct spinlock lock;

  struct data_block *prev; // LRU cache list
  struct data_block *next;

  void *data;
} data_block_t;

#define B_VALID 0x2  // buffer has been read from disk
#define B_DIRTY 0x4  // buffer needs to be written to disk

typedef struct bcache_stats {
	unsigned int hits; 
	unsigned int read_misses; 
	unsigned int write_misses; 
	unsigned int write_backs; 
} bcache_stats_t; 

typedef struct bcache {
	struct spinlock lock;
	struct spinlock iolock;

	data_block_t data_blks[DATA_BLKS_PER_DB];
	int fd; 
	bcache_stats_t stats; 

	// Linked list of all buffers, through prev/next.
  	// head.next is most recently used.
  	data_block_t head;
} bcache_t;

void binit(bcache_t *bcache);
data_block_t *bget(struct table *table, unsigned int blk_num);
data_block_t* bread(struct table *table, unsigned int blk_num);
int bflush(struct table *table);
void bwrite(data_block_t *b);
void brelse(data_block_t *b);
void bcache_stats_read_and_reset(bcache_t *bcache, bcache_stats_t *stats);
void bcache_stats_printf(bcache_stats_t *stats);
void bcache_info_printf(struct table *table);

