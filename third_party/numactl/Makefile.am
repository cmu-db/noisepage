
ACLOCAL_AMFLAGS = -I m4
CLEANFILES =

AM_CPPFLAGS = -Wall

bin_PROGRAMS = numactl numastat numademo migratepages migspeed memhog

lib_LTLIBRARIES = libnuma.la

include_HEADERS = numa.h numacompat1.h numaif.h

noinst_HEADERS = numaint.h util.h

dist_man_MANS = move_pages.2 numa.3 numactl.8 numastat.8 migratepages.8 migspeed.8 memhog.8

EXTRA_DIST = README.md INSTALL.md LICENSE.GPL2 LICENSE.LGPL2.1

numactl_SOURCES = numactl.c util.c shm.c shm.h
numactl_LDADD = libnuma.la

numastat_SOURCES = numastat.c
numastat_CFLAGS = $(AM_CFLAGS) -std=gnu99

numademo_SOURCES = numademo.c stream_lib.c stream_lib.h mt.c mt.h clearcache.c clearcache.h
numademo_CPPFLAGS = $(AM_CPPFLAGS) -DHAVE_STREAM_LIB -DHAVE_MT -DHAVE_CLEAR_CACHE
numademo_CFLAGS = -O3 -ffast-math -funroll-loops
if HAVE_TREE_VECTORIZE
numademo_CFLAGS += -ftree-vectorize
endif
numademo_LDADD = libnuma.la -lm

migratepages_SOURCES = migratepages.c util.c
migratepages_LDADD = libnuma.la

migspeed_SOURCES = migspeed.c util.c
migspeed_LDADD = libnuma.la -lrt

memhog_SOURCES = memhog.c util.c
memhog_LDADD = libnuma.la

libnuma_la_CFLAGS = -g -O2 -fno-lto
libnuma_la_SOURCES = libnuma.c syscall.c distance.c affinity.c affinity.h sysfs.c sysfs.h rtnetlink.c rtnetlink.h versions.ldscript
libnuma_la_LDFLAGS = -version-info 1:0:0 -Wl,--version-script,$(srcdir)/versions.ldscript -Wl,-init,numa_init -Wl,-fini,numa_fini

check_PROGRAMS = \
	test/distance \
	test/ftok \
	test/mbind_mig_pages \
	test/migrate_pages \
	test/move_pages \
	test/mynode \
	test/node-parse \
	test/nodemap \
	test/pagesize \
	test/prefered \
	test/randmap \
	test/realloc_test \
	test/tbitmap \
	test/tshared

EXTRA_DIST += \
	test/README \
	test/bind_range \
	test/checkaffinity \
	test/checktopology \
	test/numademo \
	test/printcpu \
	test/regress \
	test/regress2 \
	test/regress-io \
	test/runltp \
	test/shmtest

test_distance_SOURCES = test/distance.c
test_distance_LDADD = libnuma.la

test_ftok_SOURCES = test/ftok.c
test_ftok_LDADD = libnuma.la

test_mbind_mig_pages_SOURCES = test/mbind_mig_pages.c
test_mbind_mig_pages_LDADD = libnuma.la

test_migrate_pages_SOURCES = test/migrate_pages.c
test_migrate_pages_LDADD = libnuma.la

test_move_pages_SOURCES = test/move_pages.c
test_move_pages_LDADD = libnuma.la

test_mynode_SOURCES = test/mynode.c
test_mynode_LDADD = libnuma.la

test_node_parse_SOURCES = test/node-parse.c util.c
test_node_parse_LDADD = libnuma.la

test_nodemap_SOURCES = test/nodemap.c
test_nodemap_LDADD = libnuma.la

test_pagesize_SOURCES = test/pagesize.c
test_pagesize_LDADD = libnuma.la

test_prefered_SOURCES = test/prefered.c
test_prefered_LDADD = libnuma.la

test_randmap_SOURCES = test/randmap.c
test_randmap_LDADD = libnuma.la

test_realloc_test_SOURCES = test/realloc_test.c
test_realloc_test_LDADD = libnuma.la

test_tbitmap_SOURCES = test/tbitmap.c util.c
test_tbitmap_LDADD = libnuma.la

test_tshared_SOURCES = test/tshared.c
test_tshared_LDADD = libnuma.la

# Legacy make rules for test cases.
# These will be superceded by "make check".

regress1: $(check_PROGRAMS)
	cd test && ./regress

regress2: $(check_PROGRAMS)
	cd test && ./regress2

test_numademo: numademo
	./numademo -t -e 10M

test: all $(check_PROGRAMS) regress1 regress2 test_numademo

TESTS_ENVIRONMENT = builddir='$(builddir)'; export builddir;

TESTS = \
	test/bind_range \
	test/checkaffinity \
	test/checktopology \
	test/distance \
	test/move_pages \
	test/nodemap \
	test/numademo \
	test/regress \
	test/tbitmap

# These are known to be broken:
#	test/prefered
#	test/randmap

SED_PROCESS = \
        $(AM_V_GEN)$(SED) \
        -e 's,@VERSION\@,$(VERSION),g' \
        -e 's,@prefix\@,$(prefix),g' \
        -e 's,@exec_prefix\@,$(exec_prefix),g' \
        -e 's,@libdir\@,$(libdir),g' \
        -e 's,@includedir\@,$(includedir),g' \
        < $< > $@ || rm $@

%.pc: %.pc.in Makefile
	$(SED_PROCESS)

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = numa.pc
EXTRA_DIST += numa.pc.in
CLEANFILES += numa.pc
