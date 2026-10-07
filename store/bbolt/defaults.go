package bbolt

// DefaultBlobCache is the default limit, in bytes, of the cache of the
// blobs of the runs in flight (see WithBlobCache).
const DefaultBlobCache = 64 << 20

// DefaultOutputCache is the default limit, in bytes, of the cache of the
// outputs of terminal runs (see WithOutputCache).
const DefaultOutputCache = 16 << 20
