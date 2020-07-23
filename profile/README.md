Run with something like:

```
rm mem.prof ; clear ; go build && ./profile -classic -keys 1000000 -classic-num-items 1000000 -classic-fpr 0.1 -mem-profile mem.prof && go tool pprof -text -noinlines mem.prof
```
