# Copyright 2009 The Go Authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

include $(GOROOT)/src/Make.$(GOARCH)

PREREQ+=redisio.a
TARG=undis
GOFILES=\
		undis.go

REDISIO_FILES=\
			  redisio.go \
			  rdefs.go

include $(GOROOT)/src/Make.cmd

format:
	gofmt -spaces=true -tabindent=false -tabwidth=4 -w $(GOFILES) $(REDISIO_FILES)

rdefs:
	./genrdefs.sh ~/other/redis | gofmt > rdefs.go

redisio.$O: $(REDISIO_FILES)
	$(QUOTED_GOBIN)/$(GC) -o $@ $?

redisio.a: redisio.$O
	rm -f $@
	$(QUOTED_GOBIN)/gopack grc $@ $?


