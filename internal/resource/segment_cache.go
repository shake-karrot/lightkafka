package resource

import (
	"container/list"
	"sync"

	"lightkafka/internal/segment"
)

// SegmentCache manages open read-only segments system-wide.
// It limits the number of open file descriptors.
type SegmentCache struct {
	mu       sync.Mutex
	capacity int
	lruList  *list.List
	items    map[string]*list.Element // Key: "topic-partID-baseOffset"
}

type cacheItem struct {
	key string
	seg *segment.Segment
}

func NewSegmentCache(capacity int) *SegmentCache {
	if capacity <= 0 {
		capacity = 500
	}
	return &SegmentCache{
		capacity: capacity,
		lruList:  list.New(),
		items:    make(map[string]*list.Element),
	}
}

/* GetOrLoad */
func (c *SegmentCache) GetOrLoad(
	key string,
	loader func() (*segment.Segment, error),
) (*segment.Segment, error) {

	c.mu.Lock()
	defer c.mu.Unlock()

	// If the segment is in the cache, move it to the front of the list.
	if elem, ok := c.items[key]; ok {
		c.lruList.MoveToFront(elem)
		return elem.Value.(*cacheItem).seg, nil
	}

	// If the segment is not in the cache, load it using the provided loader.
	seg, err := loader()
	if err != nil {
		return nil, err
	}

	// If the cache is full, evict the least recently used segment.
	if c.lruList.Len() >= c.capacity {
		c.evict()
	}

	// Put the segment in the cache.
	item := &cacheItem{key: key, seg: seg}
	elem := c.lruList.PushFront(item)
	c.items[key] = elem

	return seg, nil
}

func (c *SegmentCache) evict() {
	elem := c.lruList.Back()
	if elem == nil {
		return
	}
	c.lruList.Remove(elem)
	item := elem.Value.(*cacheItem)
	delete(c.items, item.key)

	// Close the resource
	_ = item.seg.Close()
}

func (c *SegmentCache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for e := c.lruList.Front(); e != nil; e = e.Next() {
		item := e.Value.(*cacheItem)
		_ = item.seg.Close()
	}
	c.lruList.Init()
	c.items = make(map[string]*list.Element)
	return nil
}
