package partition

import (
	"container/list"
	"sync"

	"lightkafka/internal/segment"
)

// LRUItem holds the segment and its ID.
type LRUItem struct {
	BaseOffset int64
	Segment    *segment.Segment
}

// SegmentCache implements a thread-safe LRU cache for open segments.
type SegmentCache struct {
	mu       sync.Mutex
	capacity int

	// lruList: 가장 최근에 사용한 것이 Front, 오래된 것이 Back
	lruList *list.List
	// items: BaseOffset -> Linked List Element 매핑 (O(1) 접근용)
	items map[int64]*list.Element
}

func NewSegmentCache(capacity int) *SegmentCache {
	return &SegmentCache{
		capacity: capacity,
		lruList:  list.New(),
		items:    make(map[int64]*list.Element),
	}
}

// Get returns the segment if it exists and moves it to the front.
func (c *SegmentCache) Get(baseOffset int64) *segment.Segment {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[baseOffset]; ok {
		// Hit: 리스트의 맨 앞으로 이동 (Most Recently Used)
		c.lruList.MoveToFront(elem)
		return elem.Value.(*LRUItem).Segment
	}
	return nil
}

// Put adds a segment to the cache. Evicts the oldest if full.
func (c *SegmentCache) Put(baseOffset int64, seg *segment.Segment) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 이미 존재하면 업데이트하고 앞으로 이동
	if elem, ok := c.items[baseOffset]; ok {
		c.lruList.MoveToFront(elem)
		elem.Value.(*LRUItem).Segment = seg
		return
	}

	// 용량이 꽉 찼으면 가장 오래된 것(Back) 제거
	if c.lruList.Len() >= c.capacity {
		c.evict()
	}

	// 새 항목 추가
	item := &LRUItem{BaseOffset: baseOffset, Segment: seg}
	elem := c.lruList.PushFront(item)
	c.items[baseOffset] = elem
}

// evict removes the least recently used segment and closes it.
func (c *SegmentCache) evict() {
	elem := c.lruList.Back()
	if elem == nil {
		return
	}

	c.lruList.Remove(elem)
	item := elem.Value.(*LRUItem)
	delete(c.items, item.BaseOffset)

	_ = item.Segment.Close()
}

// Close closes all segments in the cache.
func (c *SegmentCache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for e := c.lruList.Front(); e != nil; e = e.Next() {
		item := e.Value.(*LRUItem)
		_ = item.Segment.Close()
	}

	c.lruList.Init()
	c.items = make(map[int64]*list.Element)
	return nil
}
