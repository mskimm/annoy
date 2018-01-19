ROCKSDB_VERSION=5.9.2

rocksdb:
	wget -T 10 -t 3 -c https://github.com/facebook/rocksdb/archive/v$(ROCKSDB_VERSION).tar.gz
	tar xvzf v$(ROCKSDB_VERSION).tar.gz
	ln -sfn rocksdb-$(ROCKSDB_VERSION) rocksdb
	(cd rocksdb && make static_lib)

all: rocksdb
