# Copyright 2009 The Go Authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

include $(GOROOT)/src/Make.$(GOARCH)

TARG=foo
GOFILES=\
		rdefs.go \
		undis.go

include $(GOROOT)/src/Make.cmd

tools:
	${GC} -o redis-dump.${O} redis-dump.go
	${LD} -o redis-dump redis-dump.${O}
	install -m 0755 redis-dump $(GOBIN)
	${GC} -o redis-load.${O} redis-load.go
	${LD} -o redis-load redis-load.${O}
	install -m 0755 redis-load $(GOBIN)

format:
	gofmt -spaces=true -tabindent=false -tabwidth=4 -w *.go


rdefs:
	./genrdefs.sh ~/other/redis | gofmt > rdefs.go
