#pragma once

#define DATA_BLOCK_SIZE (1 << 20) /* 1 MB for now */
#define DATA_BLKS_PER_DB 90 /* data blocks per DB */


typedef struct data_block {
  int flags;
  struct table *table;
  unsigned long blk_num;
  unsigned int refcnt;

  struct data_block *prev; // LRU cache list
  struct data_block *next;

  void *data;
} data_block_t;

#define B_VALID 0x2  // buffer has been read from disk
#define B_DIRTY 0x4  // buffer needs to be written to disk

typedef struct bcache {
	data_block_t data_blks[DATA_BLKS_PER_DB];
	int fd; 

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

