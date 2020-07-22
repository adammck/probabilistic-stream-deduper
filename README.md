# probabilistic-stream-deduper

This is a Go library which (sort of) implements a simple ring buffer of bloom
filters, to deduplicate an infinite stream of messages with bounded inaccuracy
and memory usage. This is an incomplete experiment, and should not be used by
anyone under any circumstances.
