#ifndef ANNOYROCKSDBLIB_H
#define ANNOYROCKSDBLIB_H

#include "annoylib.h"
#include <rocksdb/db.h>

namespace rocksdb {

  // expose UInt64AddOperator
  class MergeOperators {
  public:
    static std::shared_ptr<MergeOperator> CreateMaxOperator();
  };

} // namespace rocksdb

const rocksdb::Slice kNItemKey("n_items");

template<typename S, typename T, typename Distance, typename Random>
class AnnoyRocksDBIndex : public AnnoyIndexInterface<S, T> {

public:
  typedef Distance D;
  typedef typename D::template Node<S, T> Node;

protected:
  const int _f;
  Random _random;
  bool _verbose;

  // rocksdb
  rocksdb::DB* _db;
  std::vector<rocksdb::ColumnFamilyHandle*> _handles;

  rocksdb::ColumnFamilyHandle *_d_handle; // default
  rocksdb::ColumnFamilyHandle *_t_handle; // tree
  rocksdb::ColumnFamilyHandle *_i_handle; // item

  // TODO: remove this buffers for quick&dirty implementation
  T *_buffer[3];
  std::string _string_buffer;

public:

  AnnoyRocksDBIndex(int f, const char *name)
      : _f(f), _random(), _verbose(false) {

    rocksdb::Options options;
    options.create_if_missing = true;
    options.create_missing_column_families = true;

    rocksdb::ColumnFamilyOptions d_options;
    rocksdb::ColumnFamilyOptions t_options;
    rocksdb::ColumnFamilyOptions i_options;

    d_options.merge_operator = rocksdb::MergeOperators::CreateMaxOperator();

    rocksdb::ColumnFamilyDescriptor d_descriptor(rocksdb::kDefaultColumnFamilyName, d_options);
    rocksdb::ColumnFamilyDescriptor t_descriptor("t", t_options);
    rocksdb::ColumnFamilyDescriptor i_descriptor("i", i_options);

    std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;

    descriptors.push_back(d_descriptor);
    descriptors.push_back(t_descriptor);
    descriptors.push_back(i_descriptor);

    rocksdb::Status status =
        rocksdb::DB::Open(options, name, descriptors, &_handles, &_db);

    assert(status.ok());

    _d_handle = _handles[0];
    _t_handle = _handles[1];
    _i_handle = _handles[2];

    for (int i = 0 ; i < 3; i++) {
      _buffer[i] = (T *) malloc(sizeof(T) * f);
    }
  }

  ~AnnoyRocksDBIndex() {
    for (int i = 0; i < _handles.size(); i++) {
      delete _handles[i];
    }

    delete _db;

    for (int i = 0 ; i < 3; i++) {
      free(_buffer[i]);
    }
  }

  int get_f() const {
    return _f;
  }

  void add_item(S item, const T* w) override {
    add_item_impl(item, w);
  }

  template<typename W>
  void add_item_impl(S item, const W& w) {
    rocksdb::Slice key((const char *) &item, sizeof(S));
    rocksdb::Slice value((const char *) w, sizeof(W) * _f);

    _db->Put(rocksdb::WriteOptions(), _i_handle, key, value);

    // atomic update
    S num_items = item + 1;
    rocksdb::Slice num_items_value((const char *) &num_items, sizeof(S));

    _db->Merge(rocksdb::WriteOptions(), _d_handle, kNItemKey, num_items_value);
  }

  void build(int q) override {
  }

  void unbuild() override {
  }

  bool save(const char *filename) override {
    showUpdate("save won't supported\n");
    return false;
  }

  void unload() override {
    showUpdate("unload won't supported\n");
  }

  bool load(const char *filename) override {
    showUpdate("load won't supported\n");
    return false;
  }

  T get_distance(S i, S j) override {
    return 0;
  }

  void get_nns_by_item(S item, size_t n, size_t search_k, vector<S> *result,
                       vector<T> *distances) override {
  }

  void
  get_nns_by_vector(const T *w, size_t n, size_t search_k, vector<S> *result,
                    vector<T> *distances) override {
  }

  S get_n_items() override {
    std::string value;
    _db->Get(rocksdb::ReadOptions(), _d_handle, kNItemKey, &value);
    return *(S *) value.c_str();
  }

  void verbose(bool v) override {
    _verbose = v;
  }

  void get_item(S item, T *v) override {
    std::string value;
    rocksdb::Slice key((const char *) &item, sizeof(S));
    _db->Get(rocksdb::ReadOptions(), _i_handle, key, &value);
    memcpy(v, value.c_str(), _f * sizeof(T));
  }

  void set_seed(int seed) override {
    _random.set_seed(seed);
  }

};

#endif // ANNOYROCKSDBLIB_H
