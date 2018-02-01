#ifndef ANNOYROCKSDB_H
#define ANNOYROCKSDB_H

#include "../annoylib.h"

#include <rocksdb/db.h>
#include <atomic>
#include <limits>

template<typename S, typename T, typename Distance>
class RocksDBStorage : public AnnoyStorageInterface<S, T, Distance> {
protected:
  typedef typename Distance::template Node<S, T> Node;

  const int _f;
  size_t _s;
  S _K;
  bool _verbose;

  const S _tree_node_id_base;
  std::atomic<S> atomic_tree_node_id;

  // rocksdb
  rocksdb::DB* _db;
  std::vector<rocksdb::ColumnFamilyHandle*> _handles;

  // shortcut for
  rocksdb::ColumnFamilyHandle *_hd_r; // roots
  rocksdb::ColumnFamilyHandle *_hd_t; // tree nodes
  rocksdb::ColumnFamilyHandle *_hd_i; // items

  const rocksdb::WriteOptions _wo;
  const rocksdb::ReadOptions _ro;

public:

  RocksDBStorage(int f, const char *name)
      : _f(f), _tree_node_id_base(std::numeric_limits<S>::max() / 2), _wo(), _ro() {
    _s = offsetof(Node, v) + f * sizeof(T); // Size of each node
    _K = (_s - offsetof(Node, children)) / sizeof(S); // Max number of descendants to fit into node
    _verbose = false;

    rocksdb::Options options;
    options.create_if_missing = true;
    options.create_missing_column_families = true;

    // persist roots
    rocksdb::ColumnFamilyDescriptor d_descriptor(
        rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions());
    // persist tree nodes
    rocksdb::ColumnFamilyDescriptor t_descriptor(
        "t", rocksdb::ColumnFamilyOptions());
    // persist items
    rocksdb::ColumnFamilyDescriptor i_descriptor(
        "i", rocksdb::ColumnFamilyOptions());

    std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;

    descriptors.push_back(d_descriptor);
    descriptors.push_back(t_descriptor);
    descriptors.push_back(i_descriptor);

    // open db with three column families
    // _handles and _db should be released in the destructor.
    rocksdb::Status status =
        rocksdb::DB::Open(options, name, descriptors, &_handles, &_db);

    assert(status.ok());

    _hd_r = _handles[0];
    _hd_t = _handles[1];
    _hd_i = _handles[2];

    atomic_tree_node_id = std::max(_tree_node_id_base, _get_last_key(_hd_t));
  }

  virtual ~RocksDBStorage() {
    unload();
  }

  void add_item(S item, const T *v) override {
    if (item >= _tree_node_id_base) {
      return;
    }

    Node* n = alloc_node(); // TODO: memory pool
    n->children[0] = 0;
    n->children[1] = 0;
    n->n_descendants = 1;

    for (int z = 0; z < _f; z++)
      n->v[z] = v[z];

    item = _swap_endian(item);
    const rocksdb::Slice key = rocksdb::Slice((const char *)&item, sizeof(S));
    const rocksdb::Slice value = rocksdb::Slice((const char *)n, _s);
    _db->Put(_wo, _hd_i, key, value);

    free_node(n);
  }

  bool is_mutable() override {
    return true;
  }

  S append_node(const Node *node) override {
    S tree_node_id = atomic_tree_node_id++;
    S tree_node_id_be = _swap_endian(tree_node_id);
    const rocksdb::Slice key = rocksdb::Slice((const char *)&tree_node_id_be, sizeof(S));
    const rocksdb::Slice value = rocksdb::Slice((const char *)node, _s);
    _db->Put(_wo, _hd_t, key, value);
    return tree_node_id;
  }

  S append_node(const vector<S> &indices) override {
    Node *m = alloc_node(); // TODO: memory pool
    m->n_descendants = (S) indices.size();
    memcpy(m->children, &indices[0], indices.size() * sizeof(S));
    S r = append_node(m);
    free_node(m);
    return r;
  }

  void set_roots(vector<S> &roots) override {
    // purge roots from _db
    _db->DropColumnFamily(_hd_r);
    for (size_t i = 0; i < roots.size(); i++) {
      S be = _swap_endian(i);
      const rocksdb::Slice key = rocksdb::Slice((const char *)&be, sizeof(S));
      const rocksdb::Slice value = rocksdb::Slice((const char *)(&roots[i]), sizeof(S));
      _db->Put(_wo, _hd_r, key, value);
    }
  }

  Node *alloc_node() override {
    return (Node *)malloc(_s);
  }

  void free_node(Node *n) override {
    free(n);
  }

  bool load(vector<S> &roots) override {
    roots.clear();

    rocksdb::Iterator *iter = _db->NewIterator(_ro, _hd_r);
    iter->SeekToFirst();
    while (iter->Valid()) {
      roots.push_back(*(S *)iter->value().data());
      iter->Next();
    }
    delete iter;
    return true;
  }

  void unload() override {
    for (int i = 0; i < _handles.size(); i++) {
      if (_handles[i]) {
        delete _handles[i];
        _handles[i] = NULL;
      }
    }
    if (_db) {
      delete _db;
      _db = NULL;
    }
  }

  // TODO: node pool
  Node *ref_node(S i) override {
    rocksdb::ColumnFamilyHandle *hd = _hd_i;
    if (i >= _tree_node_id_base) {
      hd = _hd_t;
    }

    i = _swap_endian(i);
    const rocksdb::Slice &key = rocksdb::Slice((const char *)&i, sizeof(S));
    std::string node;
    _db->Get(_ro, hd, key, &node);
    Node *m = alloc_node();
    memcpy(m, node.c_str(), _s);
    return m;
  }

  void unref_node(Node *node) override {
    free_node(node);
  }

  void get_item(S item, T *v) override {
    item = _swap_endian(item);
    const rocksdb::Slice key = rocksdb::Slice((const char *)&item, sizeof(S));
    std::string node;
    _db->Get(_ro, _hd_i, key, &node);
    Node *m = (Node *)node.c_str();
    memcpy(v, m->v, _f * sizeof(T));
  }

  S get_n_items() override {
    return _get_last_key(_hd_i) + 1; // zero-based
  }

  S get_n_nodes() override {
    return get_n_items() + atomic_tree_node_id - _tree_node_id_base;
  }

  S max_descendants() override {
    return _K;
  }

protected:
  // http://stackoverflow.com/a/4956493/238609
  S _swap_endian(S u)
  {
    union
    {
      S u;
      unsigned char u8[sizeof(S)];
    } source, dest;

    source.u = u;

    for (size_t k = 0; k < sizeof(S); k++)
      dest.u8[k] = source.u8[sizeof(S) - k - 1];

    return dest.u;
  }

  S _get_last_key(rocksdb::ColumnFamilyHandle *hd) {
    // load _n_items
    S last = 0;
    rocksdb::Iterator *it = _db->NewIterator(_ro, hd);
    it->SeekToLast();
    if (it->Valid()) {
      last = _swap_endian(*(S *) it->key().data());
    }
    delete it;
    return last;
  }

};

#endif //ANNOYROCKSDB_H
