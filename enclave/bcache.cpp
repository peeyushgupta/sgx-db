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
#include "util.hpp"

#define VERBOSE_BCACHE 0
#define BCACHE_STATS_VERBOSE	0
void binit(bcache_t *bcache)
{
	data_block_t *b;

	initlock(&bcache->lock, std::string("bcache"));
	initlock(&bcache->iolock, std::string("bcache io"));

	// Create linked list of buffers
	bcache->head.prev = &bcache->head;
	bcache->head.next = &bcache->head;

	for(int i = 0; i < DATA_BLKS_PER_DB; i++) {
		b = &bcache->data_blks[i];
		b->next = bcache->head.next;
		b->prev = &bcache->head;
		initlock(&b->lock, std::string("block"));
		bcache->head.next->prev = b;
		bcache->head.next = b;
	}

	bcache->stats.hits = 0; 
	bcache->stats.read_misses = 0; 
	bcache->stats.write_misses = 0; 
	bcache->stats.write_backs = 0; 
}

void bcache_stats_read_and_reset(bcache_t *bcache, bcache_stats_t *stats) {

	stats->hits = bcache->stats.hits; 
	bcache->stats.hits = 0; 

	stats->read_misses = bcache->stats.read_misses; 
	bcache->stats.read_misses = 0; 
	
	stats->write_misses = bcache->stats.write_misses; 
	bcache->stats.write_misses = 0; 

	stats->write_backs = bcache->stats.write_backs; 
	bcache->stats.write_backs = 0; 

	return; 
}; 

void bcache_info_printf(struct table *table) {
	DBG("bcache size:%d, num blocks:%d, rows per block: %d\n",
		DATA_BLKS_PER_DB*DATA_BLOCK_SIZE, DATA_BLKS_PER_DB, 
		DATA_BLOCK_SIZE / row_size(table));
	return; 
};

void bcache_stats_printf(bcache_stats_t *stats) {
	DBG_ON(BCACHE_STATS_VERBOSE, "hits:%d, read misses:%d, write misses:%d, write backs:%d\n",
		stats->hits, stats->read_misses, stats->write_misses, stats->write_backs);
	return; 
};

static inline void bcache_stats_hit(bcache_t *bcache) {
	__sync_fetch_and_add(&bcache->stats.hits, 1); 
	return; 
};

static inline void bcache_stats_read_miss(bcache_t *bcache) {
	__sync_fetch_and_add(&bcache->stats.read_misses, 1); 
	return; 
};

static inline void bcache_stats_write_miss(bcache_t *bcache) {
	__sync_fetch_and_add(&bcache->stats.write_misses, 1); 
	return; 
};

static inline void bcache_stats_write_back(bcache_t *bcache) {
	__sync_fetch_and_add(&bcache->stats.write_backs, 1); 
	return; 
};


// Look through buffer cache for a block with a specific number
// If not found, allocate a buffer
// If successfully found a buffer or managed to get a new one 
// return a locked buffer, if not return NULL
data_block_t *bget(struct table *table, unsigned int blk_num)
{
	data_block_t *b;
	int ret; 

	bcache_t *bcache = &table->db->bcache;

	acquire(&bcache->lock);

	// Is the block already cached?
	for(b = bcache->head.next; b != &bcache->head; b = b->next){
		if(b->table == table && b->blk_num == blk_num){
			//unsigned long refcnt = b->refcnt;
			// Atomic increment, relies on GCC builtins
			__sync_fetch_and_add(&b->refcnt, 1);
			
			//ERR_ON(refcnt == b->refcnt, "error refcnt (%d) == b->refcnt (%d)\n", 
			//	refcnt, b->refcnt);
			//WARN_ON(refcnt + 1 != b->refcnt, "error refcnt + 1 (%d) != b->refcnt (%d)\n", 
			//	refcnt + 1, b->refcnt);
 
			
			release(&bcache->lock);
			acquire(&b->lock);
			//release(&bcache->lock);

			DBG_ON(VERBOSE_BCACHE, "blk:%p (flags:%x), num:%d, b->table:%s for table:%s\n", 
				b, b->flags, blk_num, b->table->name.c_str(), table->name.c_str());
			
			bcache_stats_hit(bcache); 
			return b;
		}
	}

	// Not cached; recycle an unused buffer.
	for(b = bcache->head.prev; b != &bcache->head; b = b->prev){
		if(b->refcnt == 0) {
			if(b->flags & B_DIRTY) {
				DBG_ON(VERBOSE_BCACHE, "write back dirty block: %p (data:%p), num:%lu, table:%s\n", 
					b, b->data, b->blk_num, b->table->name.c_str()); 
				ret = write_data_block(b->table, b->blk_num, b->data);
				if (ret) {
					ERR("writing dirty block:%lu for table %s\n", 
						b->blk_num, b->table->name.c_str());
					
 					release(&bcache->lock);
					//acquire(&b->lock);
					return NULL;
				}
				bcache_stats_write_back(bcache);
			}

			DBG_ON(VERBOSE_BCACHE, "re-using blk:%p (flags:%x), num:%d, b->table:%s for table:%s\n", 
				b, b->flags, blk_num, b->table ? b->table->name.c_str() : "NULL", table->name.c_str()); 

			b->table = table;
			b->blk_num = blk_num;
			b->flags = 0;
			//ERR_ON(b->refcnt != 0, "ref counter (%d) != 0 blk:%p (flags:%x), num:%d, b->table:%s for table:%s\n", 
			//	b->refcnt, b, b->flags, blk_num, b->table ? b->table->name.c_str() : "NULL", table->name.c_str()); 



			b->refcnt = 1;
			release(&bcache->lock);
			acquire(&b->lock);
			//release(&bcache->lock);

			return b;
		}
	}
	ERR("panic: no buffers\n");

	/* print buffer cache */
	for(b = bcache->head.next; b != &bcache->head; b = b->next){
		ERR("table: %s, blk:%d, refcnt:%d, flags:%x\n", 
			b->table->name.c_str(), b->blk_num, b->refcnt, b->flags);
	}


	release(&bcache->lock);
	return NULL;
}

int bflush(struct table *table)
{
	data_block_t *b;
	int ret; 

	bcache_t *bcache = &table->db->bcache;

	acquire(&bcache->lock);

	// Is the block already cached?
	for(b = bcache->head.next; b != &bcache->head; b = b->next) {
		if(b->table == table && b->flags & B_DIRTY) {
			DBG_ON(VERBOSE_BCACHE, "write back dirty block: %p (data:%p), num:%lu, table:%s\n", 
					b, b->data, b->blk_num, b->table->name.c_str()); 
			ret = write_data_block(b->table, b->blk_num, b->data);
			if (ret) {
				ERR("writing dirty block:%lu for table %s\n", 
					b->blk_num, table->name.c_str()); 

				release(&bcache->lock);
				return -1;
			}
			b->flags &= ~B_DIRTY; 
			
		}
	}

	release(&bcache->lock);
	return 0;
}
// Return a buf with the contents of the indicated block.
// Note the buffer is unlocked so multiple threads can use 
// it in parallel
data_block_t* bread(table_t *table, unsigned int blk_num)
{
	data_block_t *b;
	int ret; 

	b = bget(table, blk_num);
	if(b == NULL)
		return NULL; 
	
	if((b->flags & B_VALID) == 0) {
		bcache_stats_read_miss(&table->db->bcache);
		DBG_ON(VERBOSE_BCACHE, "read block: %p (data:%p), num:%lu, table:%s\n", 
			b, b->data, b->blk_num, table->name.c_str()); 

		ret = read_data_block(table, blk_num, b->data);
		if (ret) {
			ERR("failed reading data block %d for table:%s\n", 
				blk_num, table->name.c_str());
			release(&b->lock);
			return NULL;
		}
		b->flags |= B_VALID; 
	}
	release(&b->lock);
	return b;
}

// Mark block as dirty
void bwrite(data_block_t *b)
{
	//acquire(&b->table->db->bcache.lock);
	DBG_ON(VERBOSE_BCACHE, "marking dirty blk:%p (flags:%x), num:%lu, b->table:%s\n", 
		b, b->flags, b->blk_num, b->table->name.c_str()); 
	acquire(&b->lock); 
	b->flags |= B_DIRTY;
	release(&b->lock);
	//release(&b->table->db->bcache.lock);

}

// Release a buffer.
// Move to the head of the MRU list.
void brelse(data_block_t *b)
{
	int old; 
	bcache_t *bcache = &b->table->db->bcache;
	//releasesleep(&b->lock);

	//acquire(&bcache->lock);

	acquire(&b->lock);

	//unsigned long refcnt = b->refcnt;

	old = __sync_fetch_and_sub(&b->refcnt, 1);
	
	//ERR_ON(refcnt == b->refcnt, "error refcnt (%d) == b->refcnt (%d)\n", 
	//	refcnt, b->refcnt);
	//WARN_ON(refcnt - 1 != b->refcnt, "error refcnt - 1 (%d) != b->refcnt (%d)\n", 
	//			refcnt - 1, b->refcnt);
 	if (old != 1) {
		release(&b->lock);
		//release(&bcache->lock);
		return; 
	}	

	acquire(&bcache->lock);

	// no one is waiting for it.
	b->next->prev = b->prev;
	b->prev->next = b->next;
	b->next = bcache->head.next;
	b->prev = &bcache->head;
	bcache->head.next->prev = b;
	bcache->head.next = b;

	//release(&b->lock);
	release(&bcache->lock);
	release(&b->lock);

	return;
}
