// Buffer cache (from xv6)
//
// The buffer cache is a linked list of buf structures holding
// cached copies of data blocks contents.  Caching data blocks
// in enclave's memory reduces the number of crypto operations to 
// out-of-enclave memory. 
// Interface:
// * To get a buffer for a particular disk block, call bread.
// * After changing buffer data, call bwrite to write it to disk.
// * When done with the buffer, call brelse.
// * Do not use the buffer after calling brelse.
// * Only one process at a time can use a buffer,
//     so do not keep them longer than necessary.
//
// The implementation uses two state flags internally:
// * B_VALID: the buffer data has been read from the disk.
// * B_DIRTY: the buffer data has been modified
//     and needs to be written to disk.

#include "bcache.hpp"
#include "db.hpp"

void binit(bcache_t *bcache)
{
	data_block_t *b;

	// Create linked list of buffers
	bcache->head.prev = &bcache->head;
	bcache->head.next = &bcache->head;

	for(int i = 0; i < DATA_BLKS_PER_DB; i++) {
		b = &bcache->data_blks[i];
		b->next = bcache->head.next;
		b->prev = &bcache->head;
		bcache->head.next->prev = b;
		bcache->head.next = b;
	}
}

// Look through buffer cache for a block on device dev.
// If not found, allocate a buffer.
// In either case, return locked buffer.
data_block_t *bget(struct table *table, unsigned int blk_num)
{
	data_block_t *b;
	int ret; 

	bcache_t *bcache = &table->db->bcache;

	// Is the block already cached?
	for(b = bcache->head.next; b != &bcache->head; b = b->next){
		if(b->table == table && b->blk_num == blk_num){
			b->refcnt++;
			return b;
		}
	}

	// Not cached; recycle an unused buffer.
	for(b = bcache->head.prev; b != &bcache->head; b = b->prev){
		if(b->refcnt == 0) {
			if(b->flags & B_DIRTY) {
				//printf("%s: write back dirty block, num:%lu, table:%s\n", 
				//	__func__, b->blk_num, table->name.c_str()); 
				ret = write_data_block(table, b->blk_num, b->data);
				if (ret)
					return NULL;
			}

			b->table = table;
			b->blk_num = blk_num;
			b->flags = 0;
			b->refcnt = 1;
			return b;
		}
	}
	printf("%s: panic: no buffers\n", __func__);
	return NULL;
}

// Return a locked buf with the contents of the indicated block.
data_block_t* bread(table_t *table, uint blk_num)
{
	data_block_t *b;
	int ret; 

	b = bget(table, blk_num);
	if((b->flags & B_VALID) == 0) {
		ret = read_data_block(table, blk_num, b->data);
		if (ret)
			return NULL;
		b->flags |= B_VALID; 
	}
	return b;
}

// Mark block as dirty
void bwrite(data_block_t *b)
{
	b->flags |= B_DIRTY;
}

// Release a buffer.
// Move to the head of the MRU list.
void brelse(data_block_t *b)
{
	bcache_t *bcache = &b->table->db->bcache;

	b->refcnt--;
	if (b->refcnt != 0) 
		return; 
		
	// no one is waiting for it.
	b->next->prev = b->prev;
	b->prev->next = b->next;
	b->next = bcache->head.next;
	b->prev = &bcache->head;
	bcache->head.next->prev = b;
	bcache->head.next = b;
	return;
}
