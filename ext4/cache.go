package ext4

var (
	_ Cache = &mockCache{}
)

type Cache interface {
	// Add cache data
	Add(key, value interface{}) bool

	// Get returns key's value from the cache
	Get(key interface{}) (value interface{}, ok bool)
}

type inodeCacheKey int64

type mockCache struct{}

func (c *mockCache) Add(_, _ interface{}) bool {
	return false
}

func (c *mockCache) Get(_ interface{}) (interface{}, bool) {
	return nil, false
}
